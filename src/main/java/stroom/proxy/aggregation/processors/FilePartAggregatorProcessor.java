package stroom.proxy.aggregation.processors;

import stroom.proxy.aggregation.AggregationPolicySupplier;
import stroom.proxy.aggregation.FilePartBatchTransformer;
import stroom.proxy.aggregation.model.FilePartInfo;
import stroom.proxy.aggregation.model.FilePartsBatch;
import stroom.proxy.aggregation.TopicDefinition;
import stroom.proxy.aggregation.Topics;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FilePartAggregatorProcessor extends AbstractStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartAggregatorProcessor.class);

    private final Properties streamsConfig;
    private final AggregationPolicySupplier aggregationPolicySupplier;

    public FilePartAggregatorProcessor(final Properties baseStreamsConfig,
                                       final AggregationPolicySupplier aggregationPolicySupplier) {
        LOGGER.info("Initialising streams processor {} with appId {}", getName(), getAppId());
        this.aggregationPolicySupplier = aggregationPolicySupplier;
        this.streamsConfig = new Properties();
        this.streamsConfig.putAll(baseStreamsConfig);
        this.streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, getAppId());

        streamsConfig.forEach((key, value) ->
                LOGGER.info("Setting Kafka Streams property {} for appId {} to [{}]", key, getAppId(), value.toString())
        );
    }

    @Override
    public Topology getTopology() {
        final TopicDefinition<String, FilePartInfo> feedToPartsTopic = Topics.FEED_TO_PARTS_TOPIC;
        final TopicDefinition<String, FilePartsBatch> completedBatchTopic = Topics.COMPLETED_BATCH_TOPIC;

        final String storeName = "feedToCurrentBatchStore";

        final KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);

        // TODO Hopefully a local store will suffice, but need to consider what happens if we
        // have a multi node proxy cluster and we lose a node mid batch.
        // TODO check the kvstore is backed by a change log topic
        final StoreBuilder<KeyValueStore<String, FilePartsBatch>> feedToCurrentBatchStoreBuilder =
                Stores.keyValueStoreBuilder(
                        storeSupplier,
                        completedBatchTopic.getKeySerde(),
                        completedBatchTopic.getValueSerde());

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
                .addStateStore(feedToCurrentBatchStoreBuilder)
                .stream(feedToPartsTopic.getName(), feedToPartsTopic.getConsumed())
//                .peek(KafkaUtils.buildLoggingStreamPeeker(BATCH_CREATION_APP_ID, String.class, FilePartInfo.class))
                .transform(() -> new FilePartBatchTransformer(storeName, aggregationPolicySupplier), storeName)
//                .peek(KafkaUtils.buildLoggingStreamPeeker(BATCH_CREATION_APP_ID, String.class, FilePartsBatch.class))
                .to(completedBatchTopic.getName(), completedBatchTopic.getProduced());

        return streamsBuilder.build();
    }

    @Override
    public Properties getStreamConfig() {
        return streamsConfig;
    }

}
