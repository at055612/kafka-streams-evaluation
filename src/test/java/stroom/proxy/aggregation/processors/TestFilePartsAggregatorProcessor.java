package stroom.proxy.aggregation.processors;

import kafkastreamsevaluation.util.StreamProcessor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.proxy.aggregation.AggregationPolicySupplier;
import stroom.proxy.aggregation.TopicDefinition;
import stroom.proxy.aggregation.Topics;
import stroom.proxy.aggregation.model.FilePartInfo;
import stroom.proxy.aggregation.model.FilePartRef;
import stroom.proxy.aggregation.model.FilePartsBatch;
import stroom.proxy.aggregation.policy.AggregationPolicy;
import stroom.proxy.aggregation.policy.NoAggregationPolicy;
import stroom.proxy.aggregation.policy.SizeCountAgeAggregationPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class TestFilePartsAggregatorProcessor extends AbstractStreamProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestFilePartsAggregatorProcessor.class);

    private static final int PARTS_PER_INPUT_FILE = 8;
    private static final int FEEDS_PER_INPUT_FILE = 4;
    private static final String FEED_PREFIX = "FEED_";
    private static final String FEED_1 = FEED_PREFIX + "1";
    private static final String FEED_NO_AGGREGATION = FEED_PREFIX + "NO_AGGREGATION";
    private static final String FEED_CUSTOM_AGGREGATION = FEED_PREFIX + "CUSTOM_AGGREGATION";
    private static final String[] FEED_NAMES = new String[]{FEED_1, FEED_NO_AGGREGATION, FEED_CUSTOM_AGGREGATION};


    private static final String INPUT_FILE_PATH_1 = "/some/path/some/file1.zip";
    private static final String INPUT_FILE_PATH_2 = "/some/path/some/file2.zip";
    private static final String[] INPUT_FILE_PATHS = new String[]{INPUT_FILE_PATH_1, INPUT_FILE_PATH_2};
    private static final int INPUT_PARTS_COUNT = INPUT_FILE_PATHS.length * FEED_NAMES.length * PARTS_PER_INPUT_FILE;

    private static final int MAX_SIZE_BYTES_DEFAULT = 500;
    private static final int MAX_FILE_PARTS_DEFAULT = 100;
    private static final long MAX_SIZE_BYTES_CUSTOM = Long.MAX_VALUE;
    private static final int MAX_FILE_PARTS_CUSTOM = 4;

    private final TopicDefinition<String, FilePartInfo> feedToPartsTopic = Topics.FEED_TO_PARTS_TOPIC;
    private final TopicDefinition<String, FilePartsBatch> completedBatchTopic = Topics.COMPLETED_BATCH_TOPIC;

    @Test
    public void testFileSplitting() {

//        KafkaUtils.sleep(9_000);

        runProcessorTest(feedToPartsTopic, (testDriver, consumerRecordFactory) -> {

            sendInputData(testDriver, consumerRecordFactory);

            List<ProducerRecord<String, FilePartsBatch>> filePartRecords = readAllProducerRecords(
                    completedBatchTopic, testDriver);

            Map<String, List<FilePartsBatch>> feedToBatchesMap = filePartRecords.stream()
                    .collect(Collectors.groupingBy(
                            ProducerRecord::key, Collectors.mapping(
                                    ProducerRecord::value, Collectors.toList())));

            // Check all feeds are accounted for
            Assertions
                    .assertThat(feedToBatchesMap.keySet())
                    .hasSize(FEED_NAMES.length);

            // Make sure all parts are accounted for
            Assertions
                    .assertThat(feedToBatchesMap.values()
                    .stream()
                    .flatMap(List::stream)
                    .flatMap(filePartsBatch -> filePartsBatch.getFileParts().stream())
                            .collect(Collectors.toList()))
                    .hasSize(INPUT_PARTS_COUNT);

            feedToBatchesMap.forEach((feedName, batches) -> {
                LOGGER.info("Testing feed {}", feedName);

                // make sure all batches are marked complete
                Assertions
                        .assertThat(batches.stream()
                                .peek(batch -> {
                                    LOGGER.info("{} (count: {}) (size: {}) {}",
                                            feedName, batch.getFilePartsCount(), batch.getTotalSizeBytes(), batch);
                                })
                                .map(FilePartsBatch::isComplete)
                                .collect(Collectors.toList()))
                        .containsOnly(Boolean.TRUE);

                // make sure all parts belong to the feed this batch was associated with
                Assertions
                        .assertThat(batches.stream()
                                .flatMap(batch ->
                                        batch.getFileParts().stream())
                                .map(FilePartRef::getPartBaseName)
                                .allMatch(partBaseName ->
                                        partBaseName.contains(feedName)))
                        .isTrue();

            });

            List<FilePartsBatch> feed1batches = feedToBatchesMap.get(FEED_1);
            List<FilePartsBatch> feedNoAggBatches = feedToBatchesMap.get(FEED_NO_AGGREGATION);
            List<FilePartsBatch> feedCustomAggBatches = feedToBatchesMap.get(FEED_CUSTOM_AGGREGATION);

            // no agg batches should only have one part in them
            Assertions
                    .assertThat(feedNoAggBatches.stream()
                            .map(FilePartsBatch::getFilePartsCount)
                            .collect(Collectors.toList()))
                    .containsOnly(1);

            // Default agg batches should not exceed default limits
            Assertions
                    .assertThat(feed1batches.stream()
                            .map(FilePartsBatch::getFilePartsCount)
                            .allMatch(count -> 
                                    count <= MAX_FILE_PARTS_DEFAULT))
                    .isTrue();
            Assertions
                    .assertThat(feed1batches.stream()
                            .map(FilePartsBatch::getTotalSizeBytes)
                            .allMatch(size -> 
                                    size <= MAX_SIZE_BYTES_DEFAULT))
                    .isTrue();
            
            // Custom agg batches should not exceed custom limits
            Assertions
                    .assertThat(feedCustomAggBatches.stream()
                            .map(FilePartsBatch::getFilePartsCount)
                            .allMatch(count -> 
                                    count <= MAX_FILE_PARTS_CUSTOM))
                    .isTrue();
            Assertions
                    .assertThat(feedCustomAggBatches.stream()
                            .map(FilePartsBatch::getTotalSizeBytes)
                            .allMatch(size -> 
                                    size <= MAX_SIZE_BYTES_CUSTOM))
                    .isTrue();
        });
    }


    @Override
    StreamProcessor getStreamProcessor() {
        return new FilePartAggregatorProcessor(getStreamConfigProperties(), buildAggregationPolicies());
    }

    private void sendInputData(final TopologyTestDriver testDriver,
                                     final ConsumerRecordFactory<String, FilePartInfo> consumerRecordFactory) {


        final List<KeyValue<String, FilePartInfo>> keyValues = Stream.of(INPUT_FILE_PATHS)
                .flatMap(inputFilePath ->
                        Stream.of(FEED_NAMES)
                                .flatMap(feed ->
                                        IntStream.rangeClosed(1, PARTS_PER_INPUT_FILE)
                                                .mapToObj(i ->
                                                        new KeyValue<>(feed,
                                                                new FilePartInfo(
                                                                        inputFilePath,
                                                                        i + "_" + feed, // append the feedname so we can do asserts later
                                                                        Instant.now().toEpochMilli(),
                                                                        100L)))))
                .collect(Collectors.toList());

        // repeatable 'random' shuffle
        Random random = new Random(123);
        Collections.shuffle(keyValues, random);

        Assertions
                .assertThat(keyValues)
                .hasSize(INPUT_PARTS_COUNT);

        sendMessages(testDriver, consumerRecordFactory, keyValues);
    }

    private AggregationPolicySupplier buildAggregationPolicies() {

        final AggregationPolicy defaultAggregationPolicy = new SizeCountAgeAggregationPolicy(
                MAX_SIZE_BYTES_DEFAULT,
                MAX_FILE_PARTS_DEFAULT,
                Duration.ofSeconds(10).toMillis());

        final AggregationPolicy customAggregationPolicy = new SizeCountAgeAggregationPolicy(
                MAX_SIZE_BYTES_CUSTOM,
                MAX_FILE_PARTS_CUSTOM,
                Duration.ofSeconds(10).toMillis());

        final AggregationPolicy noAggregationPolicy = NoAggregationPolicy.getInstance();

        final Map<String, AggregationPolicy> aggregationPolicyMap = new HashMap<>();

        // Make FEED_1 go straight through with no aggregation
        aggregationPolicyMap.put(FEED_NO_AGGREGATION, noAggregationPolicy);
        aggregationPolicyMap.put(FEED_CUSTOM_AGGREGATION, customAggregationPolicy);

        return new AggregationPolicySupplier(defaultAggregationPolicy, aggregationPolicyMap);
    }
}