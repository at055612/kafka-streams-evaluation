package kafkastreamsevaluation.proxy.processors;

import kafkastreamsevaluation.proxy.Constants;
import kafkastreamsevaluation.proxy.FilePartConsumptionState;
import kafkastreamsevaluation.proxy.FilePartInfo;
import kafkastreamsevaluation.proxy.serde.BooleanSerde;
import kafkastreamsevaluation.proxy.serde.FilePartConsumptionStateSerde;
import kafkastreamsevaluation.proxy.serde.FilePartInfoSerde;
import kafkastreamsevaluation.util.KafkaUtils;
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
import java.util.stream.IntStream;

public class InputFileInspector extends AbstractStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InputFileInspector.class);

    private final Properties streamsConfig;

    public InputFileInspector(final Properties baseStreamsConfig) {
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

        Serde<String> stringSerde = Serdes.String();
        Serde<Boolean> booleanSerde = new BooleanSerde();
        Serde<byte[]> byteArraySerde = Serdes.ByteArray();
        Serde<FilePartInfo> filePartInfoSerde = new FilePartInfoSerde();
        Serde<FilePartConsumptionState> filePartConsumptionStateSerde = new FilePartConsumptionStateSerde();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<byte[], String> inputFilePathsStream = streamsBuilder
                .stream(Constants.INPUT_FILE_TOPIC, Consumed.with(byteArraySerde, stringSerde));

        inputFilePathsStream
                .peek(KafkaUtils.buildLoggingStreamPeeker(getAppId(), byte[].class, String.class))
                .flatMap(this::fileInspectorFlatMapper)
                .through(Constants.FEED_TO_PARTS_TOPIC, Produced.with(stringSerde, filePartInfoSerde))
                .map(this::consumedStateMapper)
                .to(Constants.FILE_PART_CONSUMED_STATE_TOPIC, Produced.with(stringSerde, filePartConsumptionStateSerde));

        return streamsBuilder.build();
    }

    @Override
    public Properties getStreamConfig() {
        return streamsConfig;
    }

    private Iterable<KeyValue<String, FilePartInfo>> fileInspectorFlatMapper(final byte[] key, final String inputFilePath) {
        Objects.requireNonNull(inputFilePath);

        // TODO replace with code that cracks open the zip and extracts all this info
        final List<KeyValue<String, FilePartInfo>> keyValues = IntStream.rangeClosed(1, 6)
                .mapToObj(i -> {
                    FilePartInfo filePartInfo = new FilePartInfo(
                        inputFilePath,
                        "00" + i,
                        System.currentTimeMillis(),
                        1024 * i);
                    String feedName = "FEED_" + i % 3;
                    return new KeyValue<>(feedName, filePartInfo);
                })
                .collect(Collectors.toList());
        return keyValues;
    }

    /**
     * Mark each file part as not yet consumed so we can track when to delete the input file.
     */
    private KeyValue<String, FilePartConsumptionState> consumedStateMapper(
            final String feedName, final FilePartInfo filePartInfo) {

        Objects.requireNonNull(filePartInfo);
        return new KeyValue<>(
                filePartInfo.getInputFilePath(),
                new FilePartConsumptionState(filePartInfo.getBaseName(), false));
    }


}
