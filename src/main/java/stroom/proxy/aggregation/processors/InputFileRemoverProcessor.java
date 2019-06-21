package stroom.proxy.aggregation.processors;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.proxy.aggregation.InputFileRemover;
import stroom.proxy.aggregation.TopicDefinition;
import stroom.proxy.aggregation.Topics;
import stroom.proxy.aggregation.model.FilePartConsumptionState;
import stroom.proxy.aggregation.model.FilePartConsumptionStates;
import stroom.proxy.aggregation.serde.FilePartConsumptionStatesSerde;

import java.util.Properties;

public class InputFileRemoverProcessor extends AbstractStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InputFileRemoverProcessor.class);

    static final String CONSUMPTION_STATES_STORE = "ConsumptionStatesStore";

    private final Properties streamsConfig;
    private final InputFileRemover inputFileRemover;

    public InputFileRemoverProcessor(final Properties baseStreamsConfig,
                                     final InputFileRemover inputFileRemover) {
        this.inputFileRemover = inputFileRemover;
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
        final Serde<FilePartConsumptionStates> filePartConsumptionStatesSerde = new FilePartConsumptionStatesSerde();
        final TopicDefinition<String, FilePartConsumptionState> filePartConsumptionStateTopic = Topics.FILE_PART_CONSUMPTION_STATE_TOPIC;

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

////        StoreBuilder<KeyValueStore<String, FilePartConsumptionStates>> aggregatesStoreBuilder = Stores.keyValueStoreBuilder(
////                Stores.persistentKeyValueStore("xxx"),
////                filePartConsumptionStateTopic.getKeySerde(),
////                filePartConsumptionStatesSerde);
//
//        streamsBuilder.addStateStore(aggregatesStoreBuilder);

        // TODO need to ensure there is no compaction/caching of this topic as this is not
        // a changelog and thus each msg value matters
        final KStream<String, FilePartConsumptionState> filePartConsumedStateStream = streamsBuilder
                .stream(filePartConsumptionStateTopic.getName(), filePartConsumptionStateTopic.getConsumed());

//        final KTable<String, FilePartConsumptionStates> filePartConsumedStatesTable = streamsBuilder
//                .table(Constants.INPUT_FILE_CONSUMED_STATE_TOPIC,
//                        Consumed.with(stringSerde, filePartConsumptionStatesSerde));

//        final Initializer<FilePartConsumptionStates> initialiser = FilePartConsumptionStates::new;
//
//        final Aggregator<String, FilePartConsumptionState, FilePartConsumptionStates> aggregator = (key, value, aggregate) ->
//            aggregate.put(value.getPartBaseName(), value.isConsumed());

//        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(CONSUMPTION_STATES_STORE);
//
//        Materialized<String, FilePartConsumptionStates, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as(storeSupplier);
//
//                .withKeySerde(filePartConsumptionStateTopic.getKeySerde())
//                .withValueSerde(filePartConsumptionStatesSerde);


        // Done in two steps to make generics accept it.
        // Not sure why the store is of <Bytes, byte[]>
        Materialized<String, FilePartConsumptionStates, KeyValueStore<Bytes, byte[]>> materialized = Materialized
                .as(CONSUMPTION_STATES_STORE);
        materialized = materialized
                .withKeySerde(filePartConsumptionStateTopic.getKeySerde())
                .withValueSerde(filePartConsumptionStatesSerde);

        filePartConsumedStateStream
                .groupByKey(filePartConsumptionStateTopic.getSerialized())
                .aggregate(
                        FilePartConsumptionStates::new,
                        this::consumptionStateAggregator,
                        materialized
                );

        // stream the changes to the ktable so we see each aggregate as it is changed
//        consumptionStatesTable
//                .toStream()
//                .filter((key, value) -> {
//                    if (LOGGER.isDebugEnabled()) {
//                        LOGGER.debug("{} {}", key, value);
//                    }
//                    return value != null && value.haveAllBeenConsumed();
//                })
//                .foreach((inputFilePath, filePartConsumptionStates) -> {
//                    if (filePartConsumptionStates != null && filePartConsumptionStates.haveAllBeenConsumed()) {
//                        LOGGER.debug("File {} can now be deleted", inputFilePath);
//
//                        inputFileRemover.remove(inputFilePath);
//
//                    } else {
//                        LOGGER.error("Shouldn't get here!!! {} {}", inputFilePath, filePartConsumptionStates);
//                    }
//
//                    // map to a tombstone entry to remove the completed set of states
////                    return new KeyValue<>(inputFilePath, (FilePartConsumptionStates) null);
//
//
//                    // TODO may want to tombstone the aggregate topic as we no longer need the value
//                });

//        KGroupedStream<String,<Tuple2<String,Boolean>>> groupedStream = filePartConsumedStateStream
//                .mapValues((readOnlyKey, value) -> new Tuple2<>(readOnlyKey.getPartBaseName(), value))
//                .groupBy((key, value) -> key.getInputFilePath());

//                .aggregate(
//                        () -> new FilePartConsumptionStates(),
//                        (key, value, aggregate) ->
//                                aggregate.put(value._1, value._2),
//                        Materialized
//                                .as("consumedStates").
//                );
//                                .withKeySerde(stringSerde)
//                                .withValueSerde(filePartConsumptionStatesSerde));

        return streamsBuilder.build();
    }

    private FilePartConsumptionStates consumptionStateAggregator(final String inputFilePath,
                                                                 final FilePartConsumptionState filePartConsumptionState,
                                                                 final FilePartConsumptionStates existingAggregate) {
        FilePartConsumptionStates newAggregate = existingAggregate.put(
                filePartConsumptionState.getPartBaseName(),
                filePartConsumptionState.isConsumed());

        if (newAggregate != null && newAggregate.haveAllBeenConsumed()) {
            LOGGER.debug("File {} can now be deleted", inputFilePath);

            try {
                inputFileRemover.remove(inputFilePath);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Error removing input file %s", inputFilePath), e);
            }

            // tombstone the aggregate to remove it from the store
            newAggregate = null;
        }

        return newAggregate;
    }

    @Override
    public Properties getStreamConfig() {
        return streamsConfig;
    }

}
