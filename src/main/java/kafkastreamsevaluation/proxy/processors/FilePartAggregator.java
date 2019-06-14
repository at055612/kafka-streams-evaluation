package kafkastreamsevaluation.proxy.processors;

import kafkastreamsevaluation.proxy.AggregationPolicy;
import kafkastreamsevaluation.proxy.AggregationPolicySupplier;
import kafkastreamsevaluation.proxy.Constants;
import kafkastreamsevaluation.proxy.FilePartBatchTransformer;
import kafkastreamsevaluation.proxy.FilePartInfo;
import kafkastreamsevaluation.proxy.FilePartsBatch;
import kafkastreamsevaluation.proxy.serde.FilePartInfoSerde;
import kafkastreamsevaluation.proxy.serde.FilePartsBatchSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class FilePartAggregator extends AbstractStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartAggregator.class);

    private final Properties streamsConfig;

    public FilePartAggregator(final Properties baseStreamsConfig) {
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
        Serde<String> feedNameSerde = Serdes.String();
        Serde<FilePartInfo> filePartInfoSerde = FilePartInfoSerde.instance();
        Serde<FilePartsBatch> filePartsBatchSerde = FilePartsBatchSerde.instance();

        String storeName = "feedToCurrentBatchStore";

        AggregationPolicy defaultAggregationPolicy = new AggregationPolicy(
                1024L * 10,
                3,
                Duration.ofSeconds(10).toMillis());
        AggregationPolicySupplier aggregationPolicySupplier = new AggregationPolicySupplier(defaultAggregationPolicy);

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);

        // TODO Hopefully a local store will suffice, but need to consider what happens if we
        // have a multi node proxy cluster and we lose a node mid batch.
        // TODO check the kvstore is backed by a change log topic
        StoreBuilder<KeyValueStore<String, FilePartsBatch>> feedToCurrentBatchStoreBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), filePartsBatchSerde);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
                .addStateStore(feedToCurrentBatchStoreBuilder)
                .stream(Constants.FEED_TO_PARTS_TOPIC, Consumed.with(feedNameSerde, filePartInfoSerde))
//                .peek(KafkaUtils.buildLoggingStreamPeeker(BATCH_CREATION_APP_ID, String.class, FilePartInfo.class))
                .transform(() -> new FilePartBatchTransformer(storeName, aggregationPolicySupplier), storeName)
//                .peek(KafkaUtils.buildLoggingStreamPeeker(BATCH_CREATION_APP_ID, String.class, FilePartsBatch.class))
                .to(Constants.COMPLETED_BATCH_TOPIC, Produced.with(feedNameSerde, filePartsBatchSerde));

        return streamsBuilder.build();
    }

    @Override
    public Properties getStreamConfig() {
        return streamsConfig;
    }

}
