package stroom.proxy.aggregation.main;

import stroom.proxy.aggregation.policy.AggregationPolicy;
import stroom.proxy.aggregation.AggregationPolicySupplier;
import stroom.proxy.aggregation.model.FilePartInfo;
import stroom.proxy.aggregation.model.FilePartsBatch;
import stroom.proxy.aggregation.policy.NoAggregationPolicy;
import stroom.proxy.aggregation.policy.SizeCountAgeAggregationPolicy;
import stroom.proxy.aggregation.Topics;
import stroom.proxy.aggregation.processors.FilePartAggregatorProcessor;
import stroom.proxy.aggregation.processors.FilePartsBatchProcessor;
import stroom.proxy.aggregation.processors.InputFileInspectorProcessor;
import stroom.proxy.aggregation.processors.InputFileRemoverProcessor;
import kafkastreamsevaluation.util.KafkaUtils;
import kafkastreamsevaluation.util.StreamProcessor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProxyAggEndToEndExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyAggEndToEndExample.class);

    private static final String GROUP_ID_BASE = ProxyAggEndToEndExample.class.getSimpleName();

    private static final int INPUT_FILE_COUNT = 100;
    private static final int PARTS_PER_INPUT_FILE = 8;
    private static final int TOTAL_FILE_PARTS = INPUT_FILE_COUNT * PARTS_PER_INPUT_FILE;
    private static final int FEEDS_PER_INPUT_FILE = 4;
    private static final String FEED_PREFIX = "FEED_";

    private final List<StreamProcessor> allStreamProcessors = new ArrayList<>();

    private final Set<String> inputhFilePaths = new HashSet<>();
    private final AtomicLong batchCounter = new AtomicLong();
    private final AtomicLong partCounter = new AtomicLong();

    private final CountDownLatch inputFileCountDownLatch = new CountDownLatch(INPUT_FILE_COUNT);
    private final CountDownLatch consumedFilePartsCountDownLatch = new CountDownLatch(TOTAL_FILE_PARTS);

    private final Random random = new Random();

    /*
    Thoughts:

    1. Need to add in a process to walk file tree finding input files that need to be processed.  Add filename to
    InputFileTopic and mark input file as processed (e.g. rename to xxx.zip.processed), so it isn't found again. The tree
    walking would be continuous, i.e. as soon as it has walked the tree it goes back to start at
    the top again. We could skip this step if proxy just put the filename on the topic when it creates
    the file.  Probably want to make it configurable so you can enable file walking or rely on something putting
    inputFilePaths on a topic, or both.

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

    1. We need to deal with failure conditions, e.g. files that can't be inspected. The file path would need to
    go onto a bad file topic or similar so the bad file could be moved/logged/etc.

    1. Ideally we want some form of prioritisation of the filePartInfo msgs. We would want some polic(y|ies) to
    assign a priority based on feedName (and possibly the createTimeMs of the part). This may involve having a
    topic per priority and then one stream processor group per topic.  Thus certain feeds will get their
    batches formed sooner than others.  We could then assign different numbers of threads to each priority group
    though this would mean lots of threads sitting idle if there are no high priority msgs.  We would need
    some mechanism for work stealing, e.g. the medium processor shunting msgs onto the high topic when it can see
    the high topic is empty.
    This may be an option for producing to and consuming from the priority topics
    https://github.com/magro/kryo-serializers
     */

    /**
     * To run this ideally pre-create the topics listed in {@link Topics}, with more than one partition
     * each if you want to make use of multiple stream threads.
     */
    public static void main(String[] args) {
        ProxyAggEndToEndExample proxyAggEndToEndExample = new ProxyAggEndToEndExample();
        proxyAggEndToEndExample.run();
    }

    private ProxyAggEndToEndExample() {


        // Set up the input file inspector processor
        // Reads input file paths from topic, inspects the file and for each puts a list of
        // file parts onto a file parts topic, keyed by feedname
        // 1 inputFilePath msg => 1-N filePartInfo msgs
        // 1 filePartInfo msg => 1 filePartConsumptionState msg
        final Properties baseStreamsConfig = KafkaUtils.buildStreamsProperties();

        final StreamProcessor inputFileInspector = new InputFileInspectorProcessor(
                baseStreamsConfig,
                this::inputFileSplitter);
        allStreamProcessors.add(inputFileInspector);



        // Set up the file parts aggregator processor
        // Reads feedname->filePartInfo msgs and assembles batches grouped by feed
        // Completed batches are sent to the completed batch topic
        // 1 filePartInfo msg => 0/1/2 filePartBatch(completed) msgs
        final AggregationPolicySupplier aggregationPolicySupplier = buildAggregationPolicies();

        final StreamProcessor filePartsAggregator = new FilePartAggregatorProcessor(
                baseStreamsConfig,
                aggregationPolicySupplier);
        allStreamProcessors.add(filePartsAggregator);



        // Set up the batch consumer processor
        // Reads feedname->completedBatch msgs and for each batch hands it off to a batch consumer
        // impl.  Once consumed each file part in the batch is marked as complete by putting
        // a msg to the file part consumption state topic
        // TODO This will need some sort of guice provider arrangement to inject the required
        // FilePartsBatchConsumer based on config.
        // 1 filePartBatch(completed) msg => 1-N filePartConsumptionState msgs
        final StreamProcessor filePartsBatchProcessor = new FilePartsBatchProcessor(
                baseStreamsConfig,
                this::acceptCompletedBatch);
        allStreamProcessors.add(filePartsBatchProcessor);



        // Set up the input file remover processor
        // Reads the file part consumption state topic and aggregates based on input file path.
        // When all parts of a file part are marked complete it will delete the input file.
        // 1 filePartConsumptionState msg => aggregation => 0-1 calls to InputFileRemover
        final StreamProcessor inputFileRemover = new InputFileRemoverProcessor(
                baseStreamsConfig,
                this::removeInputFile);
        allStreamProcessors.add(inputFileRemover);
    }


    private void run() {

        startStreamProcessors();

//        ExecutorService loggerExecutorService = KafkaUtils.startMessageLoggerConsumer(
//                GROUP_ID_BASE + "_loggingConsumer",
//                Arrays.asList(Topics.FEED_TO_PARTS_TOPIC.getName()),
//                Serdes.String(),
//                new FilePartInfoSerde());

        // Give the stream processors time to fire up and get their partitions from the broker
        KafkaUtils.sleep(3_000);

        // Send the input test data (inputFilePaths)
        sendInputFileTestMessages();

        // wait for expected data to be fully processed
        try {
            consumedFilePartsCountDownLatch.await(300, TimeUnit.SECONDS);
            inputFileCountDownLatch.await(300, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Thread interrupted waiting for count down latches to count down", e);
            Thread.currentThread().interrupt();
        }

        stopStreamProcessors();

//        loggerExecutorService.shutdownNow();
    }


    private void startStreamProcessors() {

        allStreamProcessors.forEach(StreamProcessor::start);
    }

    private void sendInputFileTestMessages() {
        List<Future<RecordMetadata>> futures = KafkaUtils.sendMessages(
                buildInputFileTestMessages(), Serdes.ByteArray(), Serdes.String());

        futures.forEach(future -> {
            try {
                //wait for kafka to accept the message
                RecordMetadata recordMetadata = future.get();
                LOGGER.debug("Sent message - \n  topic = {}\n  partition = {}\n  offset = {}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            } catch (InterruptedException e) {
                LOGGER.error("Thread interrupted");
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private AggregationPolicySupplier buildAggregationPolicies() {

        final AggregationPolicy defaultAggregationPolicy = new SizeCountAgeAggregationPolicy(
                1000L * 20,
                500,
                Duration.ofSeconds(10).toMillis());

        final AggregationPolicy partCountLimitedAggregationPolicy = new SizeCountAgeAggregationPolicy(
                Long.MAX_VALUE,
                20,
                Duration.ofSeconds(10).toMillis());

        final AggregationPolicy noAggregationPolicy = NoAggregationPolicy.getInstance();

        final Map<String, AggregationPolicy> aggregationPolicyMap = new HashMap<>();

        // Make FEED_1 go straight through with no aggregation
        aggregationPolicyMap.put(FEED_PREFIX + "1", noAggregationPolicy);
        aggregationPolicyMap.put(FEED_PREFIX + "2", partCountLimitedAggregationPolicy);

        return new AggregationPolicySupplier(defaultAggregationPolicy, aggregationPolicyMap);
    }

    private List<ProducerRecord<byte[], String>> buildInputFileTestMessages() {

        LOGGER.info("Putting {} input files on the topic (total parts {})",
                INPUT_FILE_COUNT,
                TOTAL_FILE_PARTS);

        return IntStream.rangeClosed(1, INPUT_FILE_COUNT)
                .mapToObj(i -> {
                        String inputFilePath = "/some/path/file_" + i;
                        inputhFilePaths.add(inputFilePath);
                        return new ProducerRecord<byte[], String>(
                                Topics.INPUT_FILE_TOPIC.getName(),
                                inputFilePath);
                })
                .collect(Collectors.toList());
    }

    private void stopStreamProcessors() {
        allStreamProcessors.forEach(StreamProcessor::stop);
    }

    private List<KeyValue<String, FilePartInfo>> inputFileSplitter(final String inputFilePath) {
        // TODO replace with code that cracks open the zip and extracts all this info
        final List<KeyValue<String, FilePartInfo>> keyValues = IntStream.rangeClosed(1, PARTS_PER_INPUT_FILE)
                .mapToObj(i -> {
                    FilePartInfo filePartInfo = new FilePartInfo(
                            inputFilePath,
                            "00" + i,
                            System.currentTimeMillis(),
                            1000 + (random.nextInt(5) * 1000));
                    String feedName = FEED_PREFIX + i % FEEDS_PER_INPUT_FILE;
                    return new KeyValue<>(feedName, filePartInfo);
                })
                .collect(Collectors.toList());

        // shuffle them to provide a bit of inconsistency
        Collections.shuffle(keyValues);
        return keyValues;
    }

    private void acceptCompletedBatch(final String feedName, final FilePartsBatch filePartsBatch) {

        batchCounter.incrementAndGet();
        partCounter.addAndGet(filePartsBatch.getFilePartsCount());
        filePartsBatch.getFileParts().forEach(filePartRef ->
                consumedFilePartsCountDownLatch.countDown());

        LOGGER.info(
                "Writing feed:{} count:{} bytes:{} age:{} to the stream store. Total batch count: {}, total part Count {}",
                feedName,
                filePartsBatch.getFilePartsCount(),
                filePartsBatch.getTotalSizeBytes(),
                filePartsBatch.getAgeMs(),
                batchCounter,
                partCounter);

        // TODO implement writing to stream store

    }

    private void removeInputFile(final String inputFilePath) {
        inputhFilePaths.remove(inputFilePath);
        inputFileCountDownLatch.countDown();
        LOGGER.info("'Deleted' file {}, remaining files: {}", inputFilePath, inputhFilePaths.size());
    }


}
