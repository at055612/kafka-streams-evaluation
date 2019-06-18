package kafkastreamsevaluation.proxy.main;

import kafkastreamsevaluation.proxy.AggregationPolicy;
import kafkastreamsevaluation.proxy.AggregationPolicySupplier;
import kafkastreamsevaluation.proxy.FilePartInfo;
import kafkastreamsevaluation.proxy.FilePartsBatchConsumer;
import kafkastreamsevaluation.proxy.StreamStoreBatchConsumer;
import kafkastreamsevaluation.proxy.Topics;
import kafkastreamsevaluation.proxy.processors.FilePartAggregator;
import kafkastreamsevaluation.proxy.processors.FilePartsBatchProcessor;
import kafkastreamsevaluation.proxy.processors.InputFileInspector;
import kafkastreamsevaluation.proxy.processors.InputFileRemover;
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProxyAggEndToEndExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyAggEndToEndExample.class);

    private static final String GROUP_ID_BASE = ProxyAggBatchingExampleWithTransformer.class.getSimpleName();

    public static final int INPUT_FILE_COUNT = 100;
    public static final int PARTS_PER_INPUT_FILE = 6;

    private final List<StreamProcessor> allStreamProcessors = new ArrayList<>();

    public static void main(String[] args) {
        ProxyAggEndToEndExample proxyAggEndToEndExample = new ProxyAggEndToEndExample();
        proxyAggEndToEndExample.run();
    }

    private ProxyAggEndToEndExample() {

        // Set up the input file inspector processor
        // Reads input file paths from topic, inspects the file and for each puts a list of
        // file parts onto a file parts topic, keyed by feedname
        final Properties baseStreamsConfig = KafkaUtils.buildStreamsProperties();

        final StreamProcessor inputFileInspector = new InputFileInspector(baseStreamsConfig,
                this::inputFileSplitter);
        allStreamProcessors.add(inputFileInspector);


        // Set up the file parts aggregator processor
        // Reads feedname->filePartInfo msgs and assembles batches grouped by feed
        // Completed batches are sent to the completed batch topic
        final AggregationPolicy defaultAggregationPolicy = new AggregationPolicy(
                1024L * 10,
                3,
                Duration.ofSeconds(10).toMillis());
        final AggregationPolicySupplier aggregationPolicySupplier = new AggregationPolicySupplier(defaultAggregationPolicy);

        final StreamProcessor filePartsAggregator = new FilePartAggregator(baseStreamsConfig, aggregationPolicySupplier);
        allStreamProcessors.add(filePartsAggregator);


        // Set up the batch consumer processor
        // Reads feedname->completedBatch msgs and for each batch hands it off to a batch consumer
        // impl.  Once consumed each file part in the batch is marked as complete by putting
        // a msg to the file part consumption state topic
        // TODO This will need some sort of guice provider arrangement to inject the requiured
        // FilePartsBatchConsumer based on config.
        final FilePartsBatchConsumer filePartsBatchConsumer = new StreamStoreBatchConsumer();

        final StreamProcessor filePartsBatchProcessor = new FilePartsBatchProcessor(
                baseStreamsConfig, filePartsBatchConsumer);
        allStreamProcessors.add(filePartsBatchProcessor);


        // Set up the input file remover processor
        // Reads the file part consumption state topic and aggregates based on input file path.
        // When all parts of a file part are makred complete it will delete the input file.
        final StreamProcessor inputFileRemover = new InputFileRemover(baseStreamsConfig);
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

    private List<ProducerRecord<byte[], String>> buildInputFileTestMessages() {

        LOGGER.info("Putting {} input files on the topic (total parts {})",
                INPUT_FILE_COUNT,
                INPUT_FILE_COUNT * PARTS_PER_INPUT_FILE);

        return IntStream.rangeClosed(1, INPUT_FILE_COUNT)
                .mapToObj(i ->
                        new ProducerRecord<byte[], String>(
                                Topics.INPUT_FILE_TOPIC.getName(),
                                "/some/path/file_" + i))
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
                            1024 * i);
                    String feedName = "FEED_" + i % 3;
                    return new KeyValue<>(feedName, filePartInfo);
                })
                .collect(Collectors.toList());
        return keyValues;
    }


}
