package kafkastreamsevaluation.main;

import com.google.common.collect.Maps;
import kafkastreamsevaluation.Constants;
import kafkastreamsevaluation.util.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafkastreamsevaluation.model.BasicMessageValue;
import kafkastreamsevaluation.model.MessageValue;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class EventMatchStreamsExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventMatchStreamsExample.class);

    public static final String GROUP_ID = EventMatchStreamsExample.class.getSimpleName() + "-consumer";
    public static final String STREAMS_APP_ID = EventMatchStreamsExample.class.getSimpleName() + "-streamsApp";

    public static final String EVENT_TYPE_A = "A";
    public static final String LOCATION_1 = "location1";
    public static final String LOCATION_2 = "location2";
    public static final String LOCATION_3 = "location3";
    public static final String LOCATION_4 = "location4";
    public static final String USER_1 = "user1";
    public static final String USER_2 = "user2";
    public static final String USER_3 = "user3";

    public static void main(String[] args) {

        LOGGER.info("main called with args [{}]", Arrays.stream(args).collect(Collectors.joining(" ")));
        LOGGER.info("GroupId: [{}]", GROUP_ID);

        //Start the stream processing
        ExecutorService streamProcessingExecutorService = Executors.newSingleThreadExecutor();
        KafkaStreams kafkaStreams = startStreamProcessing(streamProcessingExecutorService);

        //Start the logging consumer for both input and alert topics
        ExecutorService loggerExecutorService = KafkaUtils.startMessageLoggerConsumer(
                GROUP_ID,
                Collections.singletonList(Constants.ALERT_TOPIC));
//                Arrays.asList(Constants.INPUT_TOPIC, Constants.ALERT_TOPIC));

        // give the consume and streams app a chance to fire up
        KafkaUtils.sleep(500);

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

        Serde<String> keySerde = Serdes.String();
        Serde<MessageValue> valueSerde = MessageValue.serde();

        KStreamBuilder builder = new KStreamBuilder();
        builder.stream(keySerde, valueSerde, Constants.INPUT_TOPIC)
                .filter(KafkaUtils.buildAlwaysTrueStreamPeeker(STREAMS_APP_ID, String.class, MessageValue.class)) //peek at the stream and log all msgs
                .filter((userId, msgVal) ->
                        msgVal.getAttrValue(BasicMessageValue.KEY_LOCATION)
                                .filter(location -> location.equals(LOCATION_1))
                                .isPresent()) //filter on a single attr in the object
                .to(keySerde, valueSerde, Constants.ALERT_TOPIC);

        final KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfig);
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
        records.add(new ProducerRecord<>(
                Constants.INPUT_TOPIC,
                USER_1,
                new BasicMessageValue(baseTime.plus(offsetMins++, ChronoUnit.MINUTES),
                        BasicMessageValue.KEY_EVENT_TYPE, EVENT_TYPE_A,
                        BasicMessageValue.KEY_LOCATION, LOCATION_1,
                        BasicMessageValue.KEY_DESCRIPTION, "This message was defined at " + Instant.now().toString()).toMessageString()
        ));

        records.add(new ProducerRecord<>(
                Constants.INPUT_TOPIC,
                USER_2,
                new BasicMessageValue(baseTime.plus(offsetMins++, ChronoUnit.MINUTES),
                        BasicMessageValue.KEY_EVENT_TYPE, EVENT_TYPE_A,
                        BasicMessageValue.KEY_LOCATION, LOCATION_2,
                        BasicMessageValue.KEY_DESCRIPTION, "This is some text" + Instant.now().toString()).toMessageString()
        ));

        records.add(new ProducerRecord<>(
                Constants.INPUT_TOPIC,
                USER_3,
                new BasicMessageValue(baseTime.plus(offsetMins++, ChronoUnit.MINUTES),
                        BasicMessageValue.KEY_EVENT_TYPE, EVENT_TYPE_A,
                        BasicMessageValue.KEY_LOCATION, LOCATION_3,
                        BasicMessageValue.KEY_DESCRIPTION, "This is some text" + Instant.now().toString()).toMessageString()
        ));

        records.add(new ProducerRecord<>(
                Constants.INPUT_TOPIC,
                USER_1,
                new BasicMessageValue(baseTime.plus(offsetMins++, ChronoUnit.MINUTES),
                        BasicMessageValue.KEY_EVENT_TYPE, EVENT_TYPE_A,
                        BasicMessageValue.KEY_LOCATION, LOCATION_4,
                        BasicMessageValue.KEY_DESCRIPTION, "This is some text" + Instant.now().toString()).toMessageString()
        ));

        return records;
    }


}
