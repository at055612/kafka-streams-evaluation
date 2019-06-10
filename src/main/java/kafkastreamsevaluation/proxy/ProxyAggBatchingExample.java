package kafkastreamsevaluation.proxy;

import com.google.common.collect.Maps;
import kafkastreamsevaluation.Constants;
import kafkastreamsevaluation.util.KafkaUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
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

public class ProxyAggBatchingExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyAggBatchingExample.class);

    private static final String GROUP_ID = ProxyAggBatchingExample.class.getSimpleName() + "-consumer";
    private static final String STREAMS_APP_ID = ProxyAggBatchingExample.class.getSimpleName() + "-streamsApp";

    private static final String FEED_TO_PARTS_TOPIC = "FeedToPartsTopic";
    private static final String FEED_TO_BATCH_TOPIC = "FeedToBatchTopic";

    /*

    Stage 1 - A process walks file tree looking for un-processed (use some kind of lock file
    to indicate processed state) input zips. On finding an un-processed zip examine its dictionary
    to find out what entries are in it, ie. how many parts. Place a msg on a PartsRefCounter topic of the
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
    Once a batch is assembeled we need to send it.  Once sent we need to add a -1 to the PartsRefCounter topic so we
    can track when all parts of an input file have been sent.

    Need to figure out how to monitor grouping progress, even if new msgs aren't coming in.  May need to create
    synthetic msgs to trigger create age threshold, but there is probably a cleaner way.





    InputFileTopic: inputFilePath -> null
    PartsRefCountTopic: inputFilePath -> n (where n is the number of parts still to process)
    FeedToPartsTopic: feedName -> inputFilePath|partBasename|datFileSize|inputFileCreateTime.
    FeedToBatchTopic: feedName|batchId -> List<inputFilePath|partBasename>

    Stage 1 - Walk file tree finding input files that need to be processed.  Add filename to
    InputFileTopic and mark input file as procesed (e.g. rename to xxx.zip.processed). The tree
    walking would be continuous, i.e. as soon as it has walked the tree it goes back to start at
    the top again.

    Stage 2 - Consume from InputFileTopic and for each msg open the input file to extract the
    number of parts and for each part get the dat file size and creation time. Add parts count
    to PartsRefCounter and add each part info set to FeedToPartsTopic.

    Stage 3 - Consume from FeedToPartsTopic and for each part add the part to the batch of parts
    for that feed. If the new part would make the batch exceed the max bytes then add the batch to
    the FeedToBatchTopic, clear the batch, then add the new part. If after adding the part to the
    batch, the part count is == to the max part count or the age of the batch (based on oldest create
    time) is above max age threshold, then add batch to FeedToBatchTopic and clear batch.

    Thoughts:

    1. Assembling batches is probably best done in lmdb as if we do it in kafka the windowing in kafka
    is too rigid for our multi-dimension batch limits (e.g. count, bytes, age), plus our current batch
    state would need to live on a topic with all feeds intermingled, so extracting a single batch would
    mean scanning over loads of feeds we don't care about.

    1. Need a way of identifying batches that have reach max age without relying on the consumption of
    new parts as a trigger.  Would need some kind of delay queue to process a batch if it hasn't already
    been processed due to another limit being reached.

    1. Rather than constantly walking the tree we could maybe use inotify to watch for changes, however
    we have a very deep tree and I am not sure if we would need one watcher per dir.

    1. If the proxy is kafka aware then there is no need for proxy-agg to walk the tree as the proxy could
    just put a msg on the topic containing the location of the file to be processed.

    1. Proxy cluster - Ideally we would have a cluster of proxies, with each connecting to a shared kafka
    cluster. The proxies would
    write to some form of shared storage that all proxy nodes could access. On receipt of some data, the
    data would be written to the shared storage and the location of the file would be put on a topic. To
    provide increased resilience if the proxy couldn't connect to the shared storage or kafka it would write
    the files to local disk until connectivity was resumed.

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
                Arrays.asList(FEED_TO_PARTS_TOPIC, FEED_TO_BATCH_TOPIC));
//                Collections.singletonList(Constants.ALERT_TOPIC));

        // give the consumer and streams app a chance to fire up before producing events
        KafkaUtils.sleep(500);

        //now produce some messages on the input topic, and make sure kafka has accepted them all

        Serde<String> feedNameSerde = Serdes.String();
        Serde<FilePartInfo> filePartInfoSerde = FilePartInfoSerde.instance();

        List<Future<RecordMetadata>> futures = KafkaUtils.sendMessages(
                buildInputRecords(), feedNameSerde, filePartInfoSerde);

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
        KafkaUtils.sleep(10_000);

        kafkaStreams.close();
        streamProcessingExecutorService.shutdownNow();
        loggerExecutorService.shutdownNow();
    }

    private static KafkaStreams startStreamProcessing(ExecutorService executorService) {

        final StreamsConfig streamsConfig = KafkaUtils.buildStreamsConfig(
                STREAMS_APP_ID,
                Maps.immutableEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1));

        Serde<String> feedNameSerde = Serdes.String();
        Serde<FilePartInfo> filePartInfoSerde = FilePartInfoSerde.instance();
        Serde<BatchKey> batchKeySerde = BatchKeySerde.instance();
        Serde<FilePartsBatch> filePartsBatchSerde = FilePartsBatchSerde.instance();

        Predicate<String, FilePartInfo> filePartInfoPeeker = KafkaUtils.buildAlwaysTrueStreamPeeker(
                STREAMS_APP_ID, String.class, FilePartInfo.class);

        KStreamBuilder builder = new KStreamBuilder();

        builder.stream(feedNameSerde, filePartInfoSerde, FEED_TO_PARTS_TOPIC)
                .filter(filePartInfoPeeker) //peek at the stream and log all msgs
                .aggregateByKey(
                        FilePartsBatch::emptyBatch,
                        (aggKey, value, aggregate) ->
                                aggregate.addFilePart(value),
                        feedNameSerde,
                        filePartsBatchSerde,
                        "FilePartsBatchKTable"
                )
                .to(FEED_TO_BATCH_TOPIC);

        final KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfig);
        kafkaStreams.setUncaughtExceptionHandler(KafkaUtils.buildUncaughtExceptionHandler(STREAMS_APP_ID));

        //Start the stream processing in a new thread
        executorService.submit(kafkaStreams::start);

        //return the KafkaStreams so it can be shut down if needs be
        return kafkaStreams;
    }

    private static List<ProducerRecord<String, FilePartInfo>> buildInputRecords() {
        ZonedDateTime baseTime = ZonedDateTime.of(
                2017, 11, 30,
                10, 0, 0, 0,
                ZoneOffset.UTC);

        List<ProducerRecord<String, FilePartInfo>> records = new ArrayList<>();

        long offsetMins = 1;

        for (int i = 0; i < 4; i++) {
            final long createTimeMs = baseTime.plusMinutes(offsetMins * i).toInstant().toEpochMilli();
            final FilePartInfo filePartInfo = new FilePartInfo(
                    "/some/path/" + i + ".zip",
                    "" + i,
                    createTimeMs,
                    i + 1000);

            String feedName = "FEED_" + i % 2;
            ProducerRecord<String, FilePartInfo> producerRecord = new ProducerRecord<>(
                    FEED_TO_PARTS_TOPIC,
                    feedName,
                    filePartInfo);
            records.add(producerRecord);
        }

        return records;
    }
}
