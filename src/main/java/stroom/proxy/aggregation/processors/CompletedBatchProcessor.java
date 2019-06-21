package stroom.proxy.aggregation.processors;

import stroom.proxy.aggregation.model.FilePartConsumptionState;
import stroom.proxy.aggregation.model.FilePartsBatch;
import stroom.proxy.aggregation.FilePartsBatchConsumer;
import stroom.proxy.aggregation.TopicDefinition;
import stroom.proxy.aggregation.Topics;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class CompletedBatchProcessor extends AbstractStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompletedBatchProcessor.class);

    private final Properties streamsConfig;
    private final FilePartsBatchConsumer filePartsBatchConsumer;


    public CompletedBatchProcessor(final Properties baseStreamsConfig,
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

        final TopicDefinition<String, FilePartsBatch> completedBatchTopic = Topics.COMPLETED_BATCH_TOPIC;
        final TopicDefinition<String, FilePartConsumptionState> filePartConsumptionStateTopic = Topics.FILE_PART_CONSUMPTION_STATE_TOPIC;

        final StreamsBuilder streamsBuilder = new StreamsBuilder();


        final KStream<String, FilePartsBatch> completedBatchStream = streamsBuilder
                .stream(completedBatchTopic.getName(), completedBatchTopic.getConsumed());

        completedBatchStream
                .flatMap(this::batchConsumer)
                .to(filePartConsumptionStateTopic.getName(), filePartConsumptionStateTopic.getProduced());

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
