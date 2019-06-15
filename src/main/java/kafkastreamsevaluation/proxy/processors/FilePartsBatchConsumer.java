package kafkastreamsevaluation.proxy.processors;

import kafkastreamsevaluation.proxy.Constants;
import kafkastreamsevaluation.proxy.FilePartsBatch;
import kafkastreamsevaluation.proxy.serde.FilePartsBatchSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FilePartsBatchConsumer extends AbstractStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartsBatchConsumer.class);

    private final Properties streamsConfig;

    public FilePartsBatchConsumer(final Properties baseStreamsConfig) {
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
        final Serde<String> stringSerde = Serdes.String();
        Serde<FilePartsBatch> filePartsBatchSerde = FilePartsBatchSerde.instance();

        final StreamsBuilder streamsBuilder = new StreamsBuilder();


        final KStream<String, FilePartsBatch> completedBatchStream = streamsBuilder
                .stream(Constants.COMPLETED_BATCH_TOPIC,
                        Consumed.with(stringSerde, filePartsBatchSerde));


        return streamsBuilder.build();
    }

    @Override
    public Properties getStreamConfig() {
        return streamsConfig;
    }

}
