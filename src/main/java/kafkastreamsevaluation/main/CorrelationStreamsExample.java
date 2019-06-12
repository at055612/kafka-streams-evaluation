package kafkastreamsevaluation.main;

import com.google.common.collect.Maps;
import io.vavr.Tuple;
import kafkastreamsevaluation.Constants;
import kafkastreamsevaluation.MessageValueTimestampExtractor;
import kafkastreamsevaluation.model.AlertValue;
import kafkastreamsevaluation.model.BasicMessageValue;
import kafkastreamsevaluation.model.MessageValue;
import kafkastreamsevaluation.util.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CorrelationStreamsExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(CorrelationStreamsExample.class);

    public static final String TOPIC_A_EVENTS = "A-events";
    public static final String TOPIC_B_EVENTS = "B-events";
    public static final String GROUP_ID = CorrelationStreamsExample.class.getSimpleName() + "-consumer";
    public static final String STREAMS_APP_ID = CorrelationStreamsExample.class.getSimpleName() + "-streamsApp";

    public static final String EVENT_TYPE_A = "A";
    public static final String EVENT_TYPE_B = "B";
    public static final String ALERT_TYPE_A_B = "A-B";

    public static final String USER_1 = "user1";
    public static final String USER_2 = "user2";
    public static final String USER_3 = "user3";

    public static final String STATE_IN = "IN";
    public static final String STATE_OUT = "OUT";

    public static void main(String[] args) {

        //TODO consider JCommander for passing args in

        LOGGER.info("main called with args [{}]", Arrays.stream(args).collect(Collectors.joining(" ")));
        LOGGER.info("GroupId: [{}]", GROUP_ID);


        //Start the stream processing
        ExecutorService streamProcessingExecutorService = Executors.newSingleThreadExecutor();
        KafkaStreams kafkaStreams = startStreamProcessing(streamProcessingExecutorService);

        //Start the logging consumer for both input and alert topics
        ExecutorService loggerExecutorService = KafkaUtils.startMessageLoggerConsumer(
                GROUP_ID,
                Arrays.asList(Constants.INPUT_TOPIC, TOPIC_A_EVENTS, TOPIC_B_EVENTS, Constants.ALERT_TOPIC));

        KafkaUtils.sleep(2_000);

        //TODO consider a KTable-KTable join of state A and state B
        //Need to consider how we deal with no explicit close of a state, e.g. time it out

        //now produce some messages on the input topic, and make sure kafka has accepted them all
        try (KafkaProducer<String, String> kafkaProducer = KafkaUtils.getKafkaProducer()) {

            List<Future<RecordMetadata>> futures = KafkaUtils.sendMessages(buildInputRecords());

            futures.forEach(future -> {
                try {
                    //wait for kafka to accept the message
                    RecordMetadata recordMetadata = future.get();
                    LOGGER.info("Sent message - \n  topic = {}\n  partition = {}\n  offset = {}",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                } catch (InterruptedException e) {
                    LOGGER.error("Thread interrupted");
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        //sleep so we can wait for the alerts to appear in the console
        KafkaUtils.sleep(20_000);

        kafkaStreams.close();
        streamProcessingExecutorService.shutdownNow();
        loggerExecutorService.shutdownNow();
    }

    private static KafkaStreams startStreamProcessing(ExecutorService executorService) {

        final StreamsConfig streamsConfig = KafkaUtils.buildStreamsConfig(
                STREAMS_APP_ID,
                Maps.immutableEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2));
//                Maps.immutableEntry(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
//                        MessageValueTimestampExtractor.class.getName())); //use event time for processing

        Serde<String> keySerde = Serdes.String();
        Serde<MessageValue> valueSerde = MessageValue.serde();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, MessageValue> allEvents = builder.stream(
                Constants.INPUT_TOPIC,
                Consumed.with(
                        keySerde,
                        valueSerde,
                        new MessageValueTimestampExtractor(),
                        Topology.AutoOffsetReset.LATEST));

        KStream<String, MessageValue> aEvents = allEvents
                .filter((userId, msgVal) ->
                        msgVal.getAttrValue(BasicMessageValue.KEY_EVENT_TYPE)
                                .filter(eventType -> eventType.equals(EVENT_TYPE_A))
                                .isPresent());

        KTable<String, MessageValue> aState = aEvents
                .groupByKey(Serialized.with(keySerde, valueSerde))
                .reduce((v1, v2) -> v2, Materialized.as("aTable"));

        KStream<String, MessageValue> bEvents = allEvents
                .filter((userId, msgVal) ->
                        msgVal.getAttrValue(BasicMessageValue.KEY_EVENT_TYPE)
                                .filter(eventType -> eventType.equals(EVENT_TYPE_B))
                                .isPresent());

        KTable<String, MessageValue> bState = bEvents
                .groupByKey(Serialized.with(keySerde, valueSerde))
                .reduce((v1, v2) -> v2, Materialized.as("bTable"));

        aState.outerJoin(bState, Tuple::of)
                .mapValues(val -> {
                    LOGGER.info("\n  A: {}\n  B: {}", val._1(), val._2());

                    return (MessageValue) new AlertValue(
                        ZonedDateTime.now(),
                        ALERT_TYPE_A_B,
                        "description",
                        Arrays.asList(val._1(), val._2()));
                })
                .toStream()
                .to(Constants.ALERT_TOPIC, Produced.with(keySerde, valueSerde));


//        KStreamBuilder builderA = new KStreamBuilder();
//        KStreamBuilder builderB = new KStreamBuilder();
//
//        KTable<String, MessageValue> stateATable = builderA.table(keySerde, valueSerde, TOPIC_A_EVENTS);
//                stateATable.filter(KafkaUtils.buildAlwaysTrueStreamPeeker(STREAMS_APP_ID)); //peek at the stream and log all msgs

//                .filter((userId, msgVal) ->
//                        msgVal.getAttrValue(BasicMessageValue.KEY_EVENT_TYPE)
//                                .filter(eventType -> eventType.equals(EVENT_TYPE_A))
//                                .isPresent())
//                .filter(KafkaUtils.buildAlwaysTrueStreamPeeker(STREAMS_APP_ID)); //peek at the stream and log all msgs


//        KTable<String, MessageValue> stateBTable = builderA.table(keySerde, valueSerde, TOPIC_B_EVENTS);
//                stateBTable.filter(KafkaUtils.buildAlwaysTrueStreamPeeker(STREAMS_APP_ID)); //peek at the stream and log all msgs
//                .filter((userId, msgVal) ->
//                        msgVal.getAttrValue(BasicMessageValue.KEY_EVENT_TYPE)
//                                .filter(eventType -> eventType.equals(EVENT_TYPE_B))
//                                .isPresent())
//                .filter(KafkaUtils.buildAlwaysTrueStreamPeeker(STREAMS_APP_ID)); //peek at the stream and log all msgs


//        stateATable.join(stateBTable, Tuple::of)
//                .filter((k,v) -> {
//                    LOGGER.info("Seen key [{}], value [{}]");
//                    return true;
//                })
//                .mapValues(val -> (MessageValue) new AlertValue(
//                        ZonedDateTime.now(),
//                        ALERT_TYPE_A_B,
//                        "description",
//                        Arrays.asList(val._1(), val._2())))
//                .toStream()
//                .to(keySerde, valueSerde, Constants.ALERT_TOPIC);


        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.setUncaughtExceptionHandler(KafkaUtils.buildUncaughtExceptionHandler(STREAMS_APP_ID));

        //Start the stream processing in a new thread
        executorService.submit(kafkaStreams::start);

        //return the KafkaStreams so it can be shut down if needs be
        return kafkaStreams;
    }

    private static List<ProducerRecord<String, String>> buildInputRecords() {
        ZonedDateTime baseTime = ZonedDateTime.of(2017, 11, 30, 10, 0, 0, 0, ZoneOffset.UTC);

        List<ProducerRecord<String, String>> records = new ArrayList<>();

        long offsetMins = 0;

        //TODO fill this in

        records.add(buildAEvent(baseTime.plusMinutes(offsetMins++), USER_1, STATE_IN)); //user1 in-
        records.add(buildBEvent(baseTime.plusMinutes(offsetMins++), USER_1, STATE_IN)); //user1 in-in
//        records.add(buildBEvent(baseTime.plusMinutes(offsetMins++), USER_2, STATE_OUT)); //user1 in-in
        records.add(buildAEvent(baseTime.plusMinutes(offsetMins++), USER_1, STATE_OUT)); //user1 out-in
        records.add(buildBEvent(baseTime.plusMinutes(offsetMins++), USER_1, STATE_OUT)); //user1 out-out

//        Collections.reverse(records);

        return records;
    }

    private static ProducerRecord<String, String> buildAEvent(final ZonedDateTime zonedDateTime,
                                                       final String userId,
                                                       final String state) {
        MessageValue messageValue = new BasicMessageValue(zonedDateTime,
                BasicMessageValue.KEY_EVENT_TYPE, EVENT_TYPE_A,
                BasicMessageValue.KEY_IN_OUT, state,
                BasicMessageValue.KEY_DESCRIPTION, "This message was defined at " + Instant.now().toString());

        return new ProducerRecord<>(Constants.INPUT_TOPIC, userId, messageValue.toMessageString());
    }

    private static ProducerRecord<String, String> buildBEvent(final ZonedDateTime zonedDateTime,
                                                       final String userId,
                                                       final String state) {
        MessageValue messageValue = new BasicMessageValue(zonedDateTime,
                BasicMessageValue.KEY_EVENT_TYPE, EVENT_TYPE_B,
                BasicMessageValue.KEY_IN_OUT, state,
                BasicMessageValue.KEY_DESCRIPTION, "This message was defined at " + Instant.now().toString());

        return new ProducerRecord<>(Constants.INPUT_TOPIC, userId, messageValue.toMessageString());
    }


}
