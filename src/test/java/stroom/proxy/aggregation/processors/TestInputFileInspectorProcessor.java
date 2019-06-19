package stroom.proxy.aggregation.processors;

import kafkastreamsevaluation.util.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.proxy.aggregation.TopicDefinition;
import stroom.proxy.aggregation.Topics;
import stroom.proxy.aggregation.model.FilePartConsumptionState;
import stroom.proxy.aggregation.model.FilePartInfo;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class TestInputFileInspectorProcessor extends AbstractStreamProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestInputFileInspectorProcessor.class);

    private static final int PARTS_PER_INPUT_FILE = 8;
    private static final int FEEDS_PER_INPUT_FILE = 4;
    private static final String FEED_PREFIX = "FEED_";

    private final TopicDefinition<byte[], String> inputFileTopic = Topics.INPUT_FILE_TOPIC;
    private final TopicDefinition<String, FilePartInfo> feedToPartsTopic = Topics.FEED_TO_PARTS_TOPIC;
    private final TopicDefinition<String, FilePartConsumptionState> filePartConsumptionStateTopic = Topics.FILE_PART_CONSUMPTION_STATE_TOPIC;

    @Test
    public void testFileSplitting() {

        runProcessorTest(inputFileTopic, (testDriver, consumerRecordFactory) -> {

            String inputFilePath1 = "/some/path/some/file1.zip";
            String inputFilePath2 = "/some/path/some/file2.zip";

            ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecordFactory.create(
                    inputFileTopic.getName(),
                    inputFilePath1);

            testDriver.pipeInput(consumerRecord);

            ConsumerRecord<byte[], byte[]> consumerRecord2 = consumerRecordFactory.create(
                    inputFileTopic.getName(),
                    inputFilePath2);

            testDriver.pipeInput(consumerRecord2);

            int inputFileCount = 2;

            List<ProducerRecord<String,FilePartInfo>> filePartRecords = readAllProducerRecords(
                    feedToPartsTopic, testDriver);

            Assertions
                    .assertThat(filePartRecords)
                    .hasSize(PARTS_PER_INPUT_FILE * inputFileCount);


            List<String> feeds = filePartRecords.stream()
                    .map(ProducerRecord::key)
                    .collect(Collectors.toList());

            Assertions
                    .assertThat(feeds)
                    .hasSize(PARTS_PER_INPUT_FILE * inputFileCount);

            Assertions
                    .assertThat(feeds.stream()
                            .distinct()
                            .collect(Collectors.toList()))
                    .hasSize(FEEDS_PER_INPUT_FILE);

            // Now look at the consumption states topic
            List<ProducerRecord<String, FilePartConsumptionState>> consumptionStates = readAllProducerRecords(
                    filePartConsumptionStateTopic, testDriver);

            Assertions
                    .assertThat(consumptionStates)
                    .hasSize(PARTS_PER_INPUT_FILE * inputFileCount);

            Assertions
                    .assertThat(consumptionStates.stream()
                            .map(ProducerRecord::key)
                            .distinct()
                            .collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(inputFilePath1, inputFilePath2);

            consumptionStates.stream()
                    .map(ProducerRecord::value)
                    .forEach(filePartConsumptionState -> {
                        Assertions
                                .assertThat(filePartConsumptionState.isConsumed())
                                .isFalse();
                    });

        });
    }

    private List<KeyValue<String, FilePartInfo>> inputFileSplitter(final String inputFilePath) {
        final List<KeyValue<String, FilePartInfo>> keyValues = IntStream.rangeClosed(1, PARTS_PER_INPUT_FILE)
                .mapToObj(i -> {
                    FilePartInfo filePartInfo = new FilePartInfo(
                            inputFilePath,
                            "00" + i,
                            System.currentTimeMillis(),
                            1000);

                    String feedName = FEED_PREFIX + i % FEEDS_PER_INPUT_FILE;
                    return new KeyValue<>(feedName, filePartInfo);
                })
                .collect(Collectors.toList());

        // shuffle them to provide a bit of inconsistency
        Collections.shuffle(keyValues);
        return keyValues;
    }

    @Override
    StreamProcessor getStreamProcessor() {
        return new InputFileInspectorProcessor(getStreamConfigProperties(), this::inputFileSplitter);
    }
}