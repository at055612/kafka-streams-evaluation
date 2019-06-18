package kafkastreamsevaluation.proxy.processors;

import kafkastreamsevaluation.proxy.model.FilePartConsumptionState;
import kafkastreamsevaluation.proxy.model.FilePartInfo;
import kafkastreamsevaluation.proxy.InputFileSplitter;
import kafkastreamsevaluation.proxy.TopicDefinition;
import kafkastreamsevaluation.proxy.Topics;
import kafkastreamsevaluation.util.KafkaUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class InputFileInspectorProcessor extends AbstractStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InputFileInspectorProcessor.class);

    private final Properties streamsConfig;
    private final InputFileSplitter inputFileSplitter;

    public InputFileInspectorProcessor(final Properties baseStreamsConfig,
                                       final InputFileSplitter inputFileSplitter) {
        this.inputFileSplitter = inputFileSplitter;
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

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final TopicDefinition<byte[], String> inputFileTopic = Topics.INPUT_FILE_TOPIC;
        final TopicDefinition<String, FilePartInfo> feedToPartsTopic = Topics.FEED_TO_PARTS_TOPIC;
        final TopicDefinition<String, FilePartConsumptionState> filePartConsumptionStateTopic = Topics.FILE_PART_CONSUMPTION_STATE_TOPIC;

        final KStream<byte[], String> inputFilePathsStream = streamsBuilder
                .stream(inputFileTopic.getName(), inputFileTopic.getConsumed());

        inputFilePathsStream
                .peek(KafkaUtils.buildLoggingStreamPeeker(getAppId(), byte[].class, String.class))
                .flatMap(this::fileInspectorFlatMapper)
                .through(feedToPartsTopic.getName(), feedToPartsTopic.getProduced())
                .map(this::consumedStateMapper)
                .to(filePartConsumptionStateTopic.getName(), filePartConsumptionStateTopic.getProduced());

        return streamsBuilder.build();
    }

    @Override
    public Properties getStreamConfig() {
        return streamsConfig;
    }

    private Iterable<KeyValue<String, FilePartInfo>> fileInspectorFlatMapper(final byte[] key, final String inputFilePath) {
        Objects.requireNonNull(inputFilePath);

        return inputFileSplitter.split(inputFilePath);
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
