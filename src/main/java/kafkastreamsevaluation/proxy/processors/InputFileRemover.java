package kafkastreamsevaluation.proxy.processors;

import kafkastreamsevaluation.proxy.Constants;
import kafkastreamsevaluation.proxy.FilePartConsumptionState;
import kafkastreamsevaluation.proxy.FilePartConsumptionStates;
import kafkastreamsevaluation.proxy.serde.FilePartConsumptionStateSerde;
import kafkastreamsevaluation.proxy.serde.FilePartConsumptionStatesSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class InputFileRemover extends AbstractStreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InputFileRemover.class);

    private final Properties streamsConfig;

    public InputFileRemover(final Properties baseStreamsConfig) {
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
//        final Serde<Boolean> booleanSerde = new BooleanSerde();
//        final Serde<FilePartRef> filePartRefSerde = new FilePartRefSerde();
        final Serde<FilePartConsumptionStates> filePartConsumptionStatesSerde = new FilePartConsumptionStatesSerde();
        final Serde<FilePartConsumptionState> filePartConsumptionStateSerde = new FilePartConsumptionStateSerde();

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        // TODO need to ensure there is no compaction/caching of this topic as this is not
        // a changelog and thus each msg value matters
        final KStream<String, FilePartConsumptionState> filePartConsumedStateStream = streamsBuilder
                .stream(Constants.FILE_PART_CONSUMED_STATE_TOPIC,
                        Consumed.with(stringSerde, filePartConsumptionStateSerde));

//        final KTable<String, FilePartConsumptionStates> filePartConsumedStatesTable = streamsBuilder
//                .table(Constants.INPUT_FILE_CONSUMED_STATE_TOPIC,
//                        Consumed.with(stringSerde, filePartConsumptionStatesSerde));

//        final Initializer<FilePartConsumptionStates> initialiser = FilePartConsumptionStates::new;
//
//        final Aggregator<String, FilePartConsumptionState, FilePartConsumptionStates> aggregator = (key, value, aggregate) ->
//            aggregate.put(value.getPartBaseName(), value.isConsumed());

        filePartConsumedStateStream
                .groupByKey(Serialized.with(stringSerde, filePartConsumptionStateSerde))
                .aggregate(
                        FilePartConsumptionStates::new,
                        (key, value, aggregate) ->
                                aggregate.put(value.getPartBaseName(), value.isConsumed()),
                        // TODO do we need a store name in here
                        Materialized.with(stringSerde, filePartConsumptionStatesSerde)
                )
                .toStream()
                .filter((key, value) -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("{} {}", key, value);
                    }
                    return value != null && value.haveAllBeenConsumed();
                })
                .foreach((inputFilePath, filePartConsumptionStates) -> {
                    if (filePartConsumptionStates != null && filePartConsumptionStates.haveAllBeenConsumed()) {
                        LOGGER.info("File {} can now be deleted", inputFilePath);
                    } else {
                        LOGGER.error("Shouldn't get here!!! {} {}", inputFilePath, filePartConsumptionStates);
                    }

                    // TODO implement the actual file deletion

                    // TODO may want to tombstone the aggregate topic as we no longer need the value
                });

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

    @Override
    public Properties getStreamConfig() {
        return streamsConfig;
    }

}
