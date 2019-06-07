package kafkastreamsevaluation.proxy;

import com.google.common.collect.Maps;
import kafkastreamsevaluation.Constants;
import kafkastreamsevaluation.model.BasicMessageValue;
import kafkastreamsevaluation.model.MessageValue;
import kafkastreamsevaluation.util.KafkaUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ProxyAggBatchingExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyAggBatchingExample.class);

    private static final String GROUP_ID = ProxyAggBatchingExample.class.getSimpleName() + "-consumer";
    private static final String STREAMS_APP_ID = ProxyAggBatchingExample.class.getSimpleName() + "-streamsApp";

    /*

    Stage 1 - A process walks file tree looking for un-processed (use some kind of lock file
    to indicate processed state) input zips. On finding an un-processed zip examine its dictionary
    to find out what entries are in it, ie. how many parts. Place a msg on a PartsCounter topic of the
    form inputFilePath -> n (where n is the number of parts). For each part place a msg on the Parts topic
    of the form inputFilePath -> partBaseName. Mark input file as processed.

    Stage 2 - A streams app consumes off the parts topic.  It opens the input zip from the msg key and determines
    the feed of the part (from the msg value). It also determines the size of the dat file for the part and
    the creation time of the input file.  This information is added to the FeedToParts topic as follows:
    feedname -> inputFilePath|partBasename|datFileSize|inputFileCreateTime.

    Stage 3 - Somehow we need to group the part files by feed name, ensuring the batches don't exceed the following:
    * part file count
    * batch size bytes
    * max age (based on input file create time)
    Once a batch is assembeled we need to send it.  Once sent we need to add a -1 to the PartsCounter topic so we
    can track when all parts of an input file have been sent.

    Need to figure out how to monitor grouping progress, even if new msgs aren't coming in.  May need to create
    synthetic msgs to trigger create age threshold, but there is probably a cleaner way.

     */


    public static void main(String[] args) {

        LOGGER.info("main called with args [{}]",
                String.join(" ", args));
        LOGGER.info("GroupId: [{}]", GROUP_ID);
        LOGGER.info("Streams AppId: [{}]", STREAMS_APP_ID);

        //Start the stream processing
        ExecutorService streamProcessingExecutorService = Executors.newSingleThreadExecutor();
        KafkaStreams kafkaStreams = startStreamProcessing(streamProcessingExecutorService);

        //Start the logging consumer for both input and alert topics
        ExecutorService loggerExecutorService = KafkaUtils.startMessageLoggerConsumer(
                GROUP_ID,
                Arrays.asList(Constants.INPUT_TOPIC, Constants.ALERT_TOPIC));
//                Collections.singletonList(Constants.ALERT_TOPIC));

        // give the consumer and streams app a chance to fire up before producing events
        KafkaUtils.sleep(300);

        //now produce some messages on the input topic, and make sure kafka has accepted them all

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
                .filter(KafkaUtils.buildAlwaysTrueStreamPeeker(STREAMS_APP_ID)) //peek at the stream and log all msgs
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
