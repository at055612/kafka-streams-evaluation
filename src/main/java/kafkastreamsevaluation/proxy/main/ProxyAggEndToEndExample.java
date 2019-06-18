package kafkastreamsevaluation.proxy.main;

import kafkastreamsevaluation.proxy.AggregationPolicy;
import kafkastreamsevaluation.proxy.AggregationPolicySupplier;
import kafkastreamsevaluation.proxy.FilePartInfo;
import kafkastreamsevaluation.proxy.FilePartsBatchConsumer;
import kafkastreamsevaluation.proxy.NoAggregationPolicy;
import kafkastreamsevaluation.proxy.SizeCountAgeAggregationPolicy;
import kafkastreamsevaluation.proxy.StreamStoreBatchConsumer;
import kafkastreamsevaluation.proxy.Topics;
import kafkastreamsevaluation.proxy.processors.FilePartAggregatorProcessor;
import kafkastreamsevaluation.proxy.processors.FilePartsBatchProcessor;
import kafkastreamsevaluation.proxy.processors.InputFileInspectorProcessor;
import kafkastreamsevaluation.proxy.processors.InputFileRemoverProcessor;
import kafkastreamsevaluation.proxy.serde.FilePartInfoSerde;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProxyAggEndToEndExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyAggEndToEndExample.class);

    private static final String GROUP_ID_BASE = ProxyAggBatchingExampleWithTransformer.class.getSimpleName();

    private static final int INPUT_FILE_COUNT = 100;
    private static final int PARTS_PER_INPUT_FILE = 8;
    private static final int FEEDS_PER_INPUT_FILE = 4;
    private static final String FEED_PREFIX = "FEED_";

    private final List<StreamProcessor> allStreamProcessors = new ArrayList<>();

    private final Set<String> inputhFilePaths = new HashSet<>();

    private final Random random = new Random();

    public static void main(String[] args) {
        ProxyAggEndToEndExample proxyAggEndToEndExample = new ProxyAggEndToEndExample();
        proxyAggEndToEndExample.run();
    }

    private ProxyAggEndToEndExample() {

        // Set up the input file inspector processor
        // Reads input file paths from topic, inspects the file and for each puts a list of
        // file parts onto a file parts topic, keyed by feedname
        final Properties baseStreamsConfig = KafkaUtils.buildStreamsProperties();

        final StreamProcessor inputFileInspector = new InputFileInspectorProcessor(
                baseStreamsConfig,
                this::inputFileSplitter);
        allStreamProcessors.add(inputFileInspector);


        // Set up the file parts aggregator processor
        // Reads feedname->filePartInfo msgs and assembles batches grouped by feed
        // Completed batches are sent to the completed batch topic
        final AggregationPolicySupplier aggregationPolicySupplier = buildAggregationPolicies();

        final StreamProcessor filePartsAggregator = new FilePartAggregatorProcessor(
                baseStreamsConfig,
                aggregationPolicySupplier);
        allStreamProcessors.add(filePartsAggregator);


        // Set up the batch consumer processor
        // Reads feedname->completedBatch msgs and for each batch hands it off to a batch consumer
        // impl.  Once consumed each file part in the batch is marked as complete by putting
        // a msg to the file part consumption state topic
        // TODO This will need some sort of guice provider arrangement to inject the requiured
        // FilePartsBatchConsumer based on config.
        final FilePartsBatchConsumer filePartsBatchConsumer = new StreamStoreBatchConsumer();

        final StreamProcessor filePartsBatchProcessor = new FilePartsBatchProcessor(
                baseStreamsConfig,
                filePartsBatchConsumer);
        allStreamProcessors.add(filePartsBatchProcessor);


        // Set up the input file remover processor
        // Reads the file part consumption state topic and aggregates based on input file path.
        // When all parts of a file part are makred complete it will delete the input file.
        final StreamProcessor inputFileRemover = new InputFileRemoverProcessor(
                baseStreamsConfig,
                this::removeInputFile);
        allStreamProcessors.add(inputFileRemover);
    }


    private void run() {

        startStreamProcessors();

        ExecutorService loggerExecutorService = KafkaUtils.startMessageLoggerConsumer(
                GROUP_ID_BASE + "_loggingConsumer",
                Arrays.asList(Topics.FEED_TO_PARTS_TOPIC.getName()),
                Serdes.String(),
                new FilePartInfoSerde());

        KafkaUtils.sleep(3_000);

        sendInputFileTestMessages();

        KafkaUtils.sleep(300_000);

        stopStreamProcessors();

        loggerExecutorService.shutdownNow();
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
                INPUT_FILE_COUNT * PARTS_PER_INPUT_FILE);

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

    private void removeInputFile(final String inputFilePath) {
        inputhFilePaths.remove(inputFilePath);
        LOGGER.info("'Deleted' file {}, remaining files: {}", inputFilePath, inputhFilePaths.size());
    }


}
