package kafkastreamsevaluation.proxy.processors;

import kafkastreamsevaluation.proxy.Constants;
import kafkastreamsevaluation.proxy.FilePartConsumptionState;
import kafkastreamsevaluation.proxy.FilePartsBatch;
import kafkastreamsevaluation.proxy.FilePartsBatchConsumer;
import kafkastreamsevaluation.proxy.serde.FilePartConsumptionStateSerde;
import kafkastreamsevaluation.proxy.serde.FilePartsBatchSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class FilePartsBatchProcessor extends AbstractStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartsBatchProcessor.class);

    private final Properties streamsConfig;
    private final FilePartsBatchConsumer filePartsBatchConsumer;


    public FilePartsBatchProcessor(final Properties baseStreamsConfig,
                                   final FilePartsBatchConsumer filePartsBatchConsumer) {
        this.filePartsBatchConsumer = Objects.requireNonNull(filePartsBatchConsumer);
        LOGGER.info("Initialising streams processor {} with appId {}", getName(), getAppId());
        this.streamsConfig = new Properties();
        this.streamsConfig.putAll(baseStreamsConfig);
        this.streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, getAppId());

        streamsConfig.forEach((key, value) ->
                LOGGER.info("Setting Kafka Streams property {} for appId {} to [{}]", key, getAppId(), value.toString())
        );
    }

    @Override
    public Topology getTopology() {
        final Serde<String> feedNameSerde = Serdes.String();
        final Serde<String> inputFilePathSerde = Serdes.String();
        final Serde<FilePartsBatch> filePartsBatchSerde = FilePartsBatchSerde.instance();
        final Serde<FilePartConsumptionState> filePartConsumptionStateSerde = new FilePartConsumptionStateSerde();

        final StreamsBuilder streamsBuilder = new StreamsBuilder();


        final KStream<String, FilePartsBatch> completedBatchStream = streamsBuilder
                .stream(Constants.COMPLETED_BATCH_TOPIC,
                        Consumed.with(feedNameSerde, filePartsBatchSerde));

        completedBatchStream
                .flatMap(this::batchConsumer)
                .to(
                        Constants.FILE_PART_CONSUMED_STATE_TOPIC,
                        Produced.with(inputFilePathSerde, filePartConsumptionStateSerde));

        return streamsBuilder.build();
    }

    private Iterable<KeyValue<String, FilePartConsumptionState>> batchConsumer(final String feedName,
                                                                               final FilePartsBatch filePartsBatch) {
        LOGGER.debug("Consuming batch feed: {}, batch part count: {}", feedName, filePartsBatch.getFilePartsCount());

        try {
            filePartsBatchConsumer.accept(feedName, filePartsBatch);
        } catch (Exception e) {
            // TODO Exception handling needs tought, maybe some kind of dead letter queue
            throw new RuntimeException(String.format(
                    "Error consuming batch %s, %s", feedName, filePartsBatch), e);
        }

        // Now the batch has been consumed we can safely mark all parts as complete
        // which may trigger the deletion of input files if all their parts are complete
        final List<KeyValue<String, FilePartConsumptionState>> filePartConsumptionStateList = filePartsBatch
                .getFileParts()
                .stream()
                .map(filePartRef -> {
                    FilePartConsumptionState consumptionState = new FilePartConsumptionState(
                            filePartRef.getPartBaseName(), true);
                    LOGGER.debug("Marking {} {} as consumed",
                            filePartRef.getInputFilePath(), filePartRef.getPartBaseName());
                    return new KeyValue<>(filePartRef.getInputFilePath(), consumptionState);
                })
                .collect(Collectors.toList());

        return filePartConsumptionStateList;
    }

    @Override
    public Properties getStreamConfig() {
        return streamsConfig;
    }

}
