package kafkastreamsevaluation.proxy;

import com.google.common.collect.Maps;
import io.vavr.Tuple2;
import kafkastreamsevaluation.proxy.serde.BatchKeySerde;
import kafkastreamsevaluation.proxy.serde.FilePartInfoSerde;
import kafkastreamsevaluation.proxy.serde.FilePartsBatchSerde;
import kafkastreamsevaluation.util.KafkaUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ProxyAggBatchingExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyAggBatchingExample.class);

    private static final String GROUP_ID = ProxyAggBatchingExample.class.getSimpleName() + "-consumer";
    private static final String BATCH_CREATION_APP_ID = ProxyAggBatchingExample.class.getSimpleName() + "-batchCreation";
    private static final String BATCH_CONSUMPTION_APP_ID = ProxyAggBatchingExample.class.getSimpleName() + "-batchConsumption";

    private static final String FEED_TO_PARTS_TOPIC = "FeedToPartsTopic";
    private static final String FEED_TO_BATCH_TOPIC = "FeedToBatchTopic";
    private static final String COMPLETED_BATCH_TOPIC = "CompletedBatchTopic";
    private static final String BATCH_CHANGE_EVENTS_TOPIC = "BatchChangeEventsTopic";

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



    Mk. 2
    -----


    InputFileTopic: inputFilePath -> null
    PartConsumptionChangeEventsTopic: inputFilePath|partBasename -> false/true
      -> change events for KTable of FilePartConsumptionStates
    FeedToPartsTopic: feedName -> FilePartInfo(inputFilePath|partBasename|datFileSize|inputFileCreateTime)
    BatchChangeEventsTopic: feedName -> BatchChangeEvent(type|FilePartInfo)
      -> change events for KTable of FilePartsBatch
    ForwardFileTopic: forwardFilePath -> null

    Stage 1 - Walk file tree finding input files that need to be processed.  Add filename to
    InputFileTopic and mark input file as processed (e.g. rename to xxx.zip.processed). The tree
    walking would be continuous, i.e. as soon as it has walked the tree it goes back to start at
    the top again. We could skip this step if proxy just put the filename on the topic when it creates
    the file.

    Stage 2 - Consume from InputFileTopic and for each msg (input file) open the input file to extract the
    number of parts and for each part get the dat file size and creation time. For each part
    add inputFilePath|partbasename -> false to PartConsumptionChangeEventsTopic and
    add each part info object to FeedToPartsTopic.

    Stage 3 - Outer join FeedToPartsTopic with KTable of FilePartsBatch so each file part is joined to latest
    picture of the batch for that feed. Flat map to a list of BatchChangeEvent objects so we can mutate the
    batch state.  If the current batch is null then map to a batch initialise event and an add for the file part.
    If the batch is incomplete but not full map to an add for the file part. If the batch is incomplete but 'ready'
    then map to a batch complete event and a batch initialise event and a batch add event. If the batch is complete
    map to a batch initialise event and an add event for the part. 'Ready' is governed by age/count/size.

    Stage 4 - Consume from the topic of batches (maybe we need to put completed batches to their own topic).
    If batch is marked complete then either create a zip from the batch ready for forwarding (and add details
    of the new zip to ForwardFileTopic) or write to the stream
    store.  Add inputFilePath|partbasename -> true for each part in the batch to indicate the parts in the
    input files are no longer needed.

    Stage 5 Consume from the KTable of FilePartConsumptionStates and whenever we get one where all parts are
    true, delete the corresponding input file.




    Thoughts:

    1. Assembling batches is probably best done in lmdb as if we do it in kafka the windowing in kafka
    is too rigid for our multi-dimension batch limits (e.g. count, bytes, age), plus our current batch
    state would need to live on a topic with all feeds intermingled, so extracting a single batch would
    mean scanning over loads of feeds we don't care about.

    1. Need a way of identifying batches that have reach max age without relying on the consumption of
    new parts as a trigger.  Would need some kind of delay queue to process a batch if it hasn't already
    been processed due to another limit being reached.  It may be possible to add initialised batches to
    another topic where the message time is set to the max age of the batch.  Then another process can
    regularly try to consume from the topic from the current time onwards rather than from an offset.  See
    https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868090

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

    1. For the left join to work the two topics MUST have identical partition counts and data must be fed
    to them with identical partitioning schemes (i.e. hashing of the key).



    kcreate FeedToBatchTopic; kconsume FeedToBatchTopic
    topic="CompletedBatchTopic"; kcreate $topic; kconsume $topic
    kcreate FeedToPartsTopic; kconsume FeedToPartsTopic


     */


    public static void main(String[] args) {

        LOGGER.info("main called with args [{}]",
                String.join(" ", args));
        LOGGER.info("GroupId: [{}]", GROUP_ID);

        Serde<String> feedNameSerde = Serdes.String();
        Serde<FilePartInfo> filePartInfoSerde = FilePartInfoSerde.instance();
        Serde<FilePartsBatch> filePartsBatchSerde = FilePartsBatchSerde.instance();

        //Start the stream processing for batch creation
        ExecutorService batchCreationExecutorService = Executors.newSingleThreadExecutor();
        KafkaStreams batchCreationKafkaStreams = startBatchCreationStreamProcessing(batchCreationExecutorService);

        ExecutorService batchConsumptionExecutorService = Executors.newSingleThreadExecutor();
        KafkaStreams batchConsumptionKafkaStreams = startBatchConsumptionStreamProcessing(batchConsumptionExecutorService);

        //Start the logging consumer for both input and alert topics
        ExecutorService loggerExecutorService = KafkaUtils.startMessageLoggerConsumer(
                GROUP_ID,
                Arrays.asList(FEED_TO_BATCH_TOPIC),
                Serdes.String(),
                filePartsBatchSerde);

        // give the consumer and streams app a chance to fire up before producing events
        KafkaUtils.sleep(2_000);

        //now produce some messages on the input topic, and make sure kafka has accepted them all


//        List<Future<RecordMetadata>> emptyBatchFutures = KafkaUtils.sendMessages(
//                buildFilePartsBatchInputRecords(), feedNameSerde, filePartsBatchSerde);
//
//        emptyBatchFutures.forEach(future -> {
//            try {
//                future.get();
//            } catch (InterruptedException | ExecutionException e) {
//                throw new RuntimeException(e);
//            }
//        });

        List<Future<RecordMetadata>> futures = KafkaUtils.sendMessages(
                buildFilePartInfoInputRecords(), feedNameSerde, filePartInfoSerde);

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
        KafkaUtils.sleep(2000_000);

        batchCreationKafkaStreams.close();
        batchCreationExecutorService.shutdownNow();

        batchConsumptionKafkaStreams.close();
        batchConsumptionExecutorService.shutdownNow();

        loggerExecutorService.shutdownNow();
    }

    private static KafkaStreams startBatchCreationStreamProcessing(ExecutorService executorService) {

        final StreamsConfig streamsConfig = KafkaUtils.buildStreamsConfig(
                BATCH_CREATION_APP_ID,
                Maps.immutableEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1));

        Serde<String> feedNameSerde = Serdes.String();
        Serde<FilePartInfo> filePartInfoSerde = FilePartInfoSerde.instance();
        Serde<BatchKey> batchKeySerde = BatchKeySerde.instance();
        Serde<FilePartsBatch> filePartsBatchSerde = FilePartsBatchSerde.instance();

        Predicate<String, FilePartInfo> filePartInfoPeeker = KafkaUtils.buildAlwaysTrueStreamPeeker(
                BATCH_CREATION_APP_ID, String.class, FilePartInfo.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, FilePartInfo> filePartInfoStream = builder
                .stream(FEED_TO_PARTS_TOPIC, Consumed.with(feedNameSerde, filePartInfoSerde));

        KTable<String, FilePartsBatch> feedToBatchTable = builder
                .table(FEED_TO_BATCH_TOPIC, Consumed.with(feedNameSerde, filePartsBatchSerde));

        Predicate<String, FilePartsBatch> batchIsCompletePredicate = (feedName, filePartsBatch) ->
                filePartsBatch.isComplete();
        Predicate<String, FilePartsBatch> acceptAllPredicate = (feedName, filePartsBatch) -> true;

//        Predicate[] branchPredicates = new Predicate[]{batchIsCompletePredicate, acceptAllPredicate};


        @SuppressWarnings("unchecked")
        KStream<String, FilePartsBatch>[] branchedStreams = filePartInfoStream
//                .filter(filePartInfoPeeker) //peek at the stream and log all msgs
                .leftJoin(feedToBatchTable, Tuple2::new)
                .flatMapValues(tuple2 -> {
                    FilePartInfo filePartInfo = tuple2._1();
                    FilePartsBatch currentBatch = tuple2._2();
                    LOGGER.debug("Seen in join filePart: {}, batch size: {}", filePartInfo.getBaseName(),
                            currentBatch == null ? "-" : currentBatch.getFilePartsCount());
                    List<FilePartsBatch> outputBatchChangeLog = new ArrayList<>();

                    if (currentBatch == null) {
                        // no batch for this feed so create one
                        FilePartsBatch newBatch = new FilePartsBatch(filePartInfo, false);
                        // TODO need to test for readiness after adding.
                        outputBatchChangeLog.add(newBatch);
                        LOGGER.debug("Created new batch, count: " + newBatch.getFilePartsCount());
                    } else {
                        if (isBatchReady(currentBatch)) {
                            FilePartsBatch completedBatch = currentBatch.completeBatch();
                            LOGGER.debug("Completing existing batch, current count: " + currentBatch.getFilePartsCount());
                            // TODO need to test for readiness after adding.
                            FilePartsBatch newBatch = new FilePartsBatch(filePartInfo, false);
                            outputBatchChangeLog.add(completedBatch);
                            outputBatchChangeLog.add(newBatch);
                            LOGGER.debug("Created new batch, count: " + newBatch.getFilePartsCount());
                        } else {
                            FilePartsBatch updatedBatch = currentBatch.addFilePart(filePartInfo);
                            LOGGER.debug("Added to existing batch, new count: " + updatedBatch.getFilePartsCount());
                            outputBatchChangeLog.add(updatedBatch);

                        }
                        // we already have a batch so test its readiness. If ready
                        // complete it, null it then , else add to it.
                    }
                    if (outputBatchChangeLog.size() > 2 || outputBatchChangeLog.size() < 1) {
                        throw new RuntimeException("Not expecting more that 2 or less than 1 batch objects here");
                    }
                    KafkaUtils.sleep(10_000);
                    LOGGER.debug("Outputting {} batches, {}", outputBatchChangeLog.size(),
                            outputBatchChangeLog.stream()
                                .map(filePartsBatch -> String.valueOf(filePartsBatch.getFilePartsCount()))
                                .collect(Collectors.joining(",")));
                    return outputBatchChangeLog;
                })
                .peek(KafkaUtils.buildLoggingStreamPeeker(BATCH_CREATION_APP_ID, String.class, FilePartsBatch.class))
                .branch(batchIsCompletePredicate, acceptAllPredicate); // branch to avoid losing batch in compaction

        // Any completed batches
        branchedStreams[0]
                .to(COMPLETED_BATCH_TOPIC, Produced.with(feedNameSerde, filePartsBatchSerde));

        // Any in-complete batches
        branchedStreams[1]
                .to(FEED_TO_BATCH_TOPIC, Produced.with(feedNameSerde, filePartsBatchSerde));


//                .foreach((key, value) -> {
//                    System.out.println(key + " - " + value);
//
//                });
//                .aggregateByKey(
//                        FilePartsBatch::emptyBatch,
//                        (aggKey, value, aggregate) ->
//                                aggregate.addFilePart(value),
//                        feedNameSerde,
//                        filePartsBatchSerde,
//                        "FilePartsBatchKTable"
//                )
//                .to(feedNameSerde, filePartInfoSerde, FEED_TO_BATCH_TOPIC);

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.setUncaughtExceptionHandler(KafkaUtils.buildUncaughtExceptionHandler(BATCH_CREATION_APP_ID));

        //Start the stream processing in a new thread
        executorService.submit(kafkaStreams::start);

        //return the KafkaStreams so it can be shut down if needs be
        return kafkaStreams;
    }

    private static KafkaStreams startBatchConsumptionStreamProcessing(ExecutorService executorService) {

        final StreamsConfig streamsConfig = KafkaUtils.buildStreamsConfig(
                BATCH_CONSUMPTION_APP_ID,
                Maps.immutableEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1));

        Serde<String> feedNameSerde = Serdes.String();
        Serde<FilePartsBatch> filePartsBatchSerde = FilePartsBatchSerde.instance();

        Predicate<String, FilePartsBatch> filePartInfoPeeker = KafkaUtils.buildAlwaysTrueStreamPeeker(
                BATCH_CONSUMPTION_APP_ID, String.class, FilePartsBatch.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, FilePartsBatch> feedToBatchStream = builder
                .stream(COMPLETED_BATCH_TOPIC, Consumed.with(feedNameSerde, filePartsBatchSerde));

        feedToBatchStream
                .filter(filePartInfoPeeker) //peek at the stream and log all msgs
                .filter((feedName, filePartsBatch) -> filePartsBatch.isComplete())
                .foreach((feedName, filePartsBatch) -> {
                    LOGGER.debug("Consuming completed batch {}: {}", feedName, filePartsBatch);
                });

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.setUncaughtExceptionHandler(KafkaUtils.buildUncaughtExceptionHandler(BATCH_CONSUMPTION_APP_ID));

        //Start the stream processing in a new thread
        executorService.submit(kafkaStreams::start);

        //return the KafkaStreams so it can be shut down if needs be
        return kafkaStreams;
    }

    private static boolean isBatchReady(final FilePartsBatch filePartsBatch) {
        Objects.requireNonNull(filePartsBatch);
        int maxFileParts = 3;
        long maxAgeMs = Duration.ofDays(100).toMillis();
        long maxSizeBytes = 10_000;
        if (filePartsBatch.getFilePartsCount() >= maxFileParts) {
            LOGGER.debug("Part count {} has reached its limit {}", filePartsBatch.getFilePartsCount(), maxFileParts);
            return true;
        } else if (filePartsBatch.getAgeMs() >= maxAgeMs) {
            LOGGER.debug("AgeMs {} has reached its limit {}", filePartsBatch.getAgeMs(), maxAgeMs);
            return true;
        } else if (filePartsBatch.getTotalSizeBytes() >= maxSizeBytes) {
            LOGGER.debug("TotalSizeBytes {} has reached its limit {}", filePartsBatch.getTotalSizeBytes(), maxSizeBytes);
            return true;
        }
        return false;
    }

    private static List<ProducerRecord<String, FilePartInfo>> buildFilePartInfoInputRecords() {
        ZonedDateTime baseTime = ZonedDateTime.of(
                2019, 6, 30,
                10, 0, 0, 0,
                ZoneOffset.UTC);

        List<ProducerRecord<String, FilePartInfo>> records = new ArrayList<>();

        long offsetMins = 1;

        for (int i = 0; i < 4; i++) {
            final String feedName = "FEED_" + i % 1;
            final long createTimeMs = baseTime.plusMinutes(offsetMins * i).toInstant().toEpochMilli();
            final FilePartInfo filePartInfo = new FilePartInfo(
                    "/some/path/" + feedName + "_" + i + ".zip",
                    "" + i,
                    createTimeMs,
                    i + 1000);

            ProducerRecord<String, FilePartInfo> producerRecord = new ProducerRecord<>(
                    FEED_TO_PARTS_TOPIC,
                    feedName,
                    filePartInfo);
            records.add(producerRecord);
        }

        return records;
    }

//    private static List<ProducerRecord<String, FilePartsBatch>> buildFilePartsBatchInputRecords() {
//
//        List<ProducerRecord<String, FilePartsBatch>> records = new ArrayList<>();
//
//        long offsetMins = 1;
//
//        for (int i = 0; i < 2; i++) {
//            String feedName = "FEED_" + i % 2;
//            ProducerRecord<String, FilePartsBatch> producerRecord = new ProducerRecord<>(
//                    FEED_TO_BATCH_TOPIC,
//                    "FEED_1",
//                    FilePartsBatch.emptyBatch());
//            records.add(producerRecord);
//        }
//
//        return records;
//    }
}
