package stroom.proxy.aggregation.processors;

import io.vavr.Tuple;
import kafkastreamsevaluation.util.StreamProcessor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.proxy.aggregation.FilePartsBatchConsumer;
import stroom.proxy.aggregation.TopicDefinition;
import stroom.proxy.aggregation.Topics;
import stroom.proxy.aggregation.model.FilePartConsumptionState;
import stroom.proxy.aggregation.model.FilePartInfo;
import stroom.proxy.aggregation.model.FilePartsBatch;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class TestCompletedBatchProcessor extends AbstractStreamProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestCompletedBatchProcessor.class);

    @Mock
    private FilePartsBatchConsumer mockFilePartsBatchConsumer;

    private final TopicDefinition<String, FilePartsBatch> completedBatchTopic = Topics.COMPLETED_BATCH_TOPIC;
    private final TopicDefinition<String, FilePartConsumptionState> filePartConsumptionStateTopic = Topics.FILE_PART_CONSUMPTION_STATE_TOPIC;

    private static final String FEED_1 = "FEED_1";
    private static final String FEED_2 = "FEED_2";
    private static final String INPUT_FILE_PATH_1 = "/some/path/some/file1.zip";
    private static final String INPUT_FILE_PATH_2 = "/some/path/some/file2.zip";

    @Test
    public void testOnePartOneBatchOnePart() {
        runProcessorTest(completedBatchTopic, (testDriver, consumerRecordFactory) -> {

            final String feed = FEED_1;
            final String inputPath = INPUT_FILE_PATH_1;
            final FilePartsBatch batch = new FilePartsBatch(
                    new FilePartInfo(
                            inputPath,
                            "1",
                            Instant.now().toEpochMilli(),
                            1024L),
                    true);

            sendMessage(testDriver, consumerRecordFactory, feed, batch);


            List<ProducerRecord<String, FilePartConsumptionState>> filePartConsumptionStates = readAllProducerRecords(
                    filePartConsumptionStateTopic, testDriver);

            Assertions
                    .assertThat(filePartConsumptionStates)
                    .hasSize(1);

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                    .map(ProducerRecord::key)
                            .collect(Collectors.toList()))
                    .contains(inputPath);

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                            .map(rec -> rec.value().getPartBaseName())
                            .collect(Collectors.toList()))
                    .contains("1");

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                            .map(rec -> rec.value().isConsumed())
                            .collect(Collectors.toList()))
                    .contains(true);

            Mockito
                    .verify(mockFilePartsBatchConsumer, Mockito.times(1))
                    .accept(Mockito.anyString(), Mockito.any());
        });
    }

    @Test
    public void testOnePartOneBatchMultipleParts() {
        runProcessorTest(completedBatchTopic, (testDriver, consumerRecordFactory) -> {

            final String feed = "MY_FEED";
            final String inputPath = INPUT_FILE_PATH_1;
            final FilePartsBatch batch = new FilePartsBatch(
                    new FilePartInfo(
                            inputPath,
                            "1",
                            Instant.now().toEpochMilli(),
                            1024L),
                    false)
                    .addFilePart(
                            new FilePartInfo(
                                    inputPath,
                                    "2",
                                    Instant.now().toEpochMilli(),
                                    1024L))
                    .completeBatch();

            sendMessage(testDriver, consumerRecordFactory, feed, batch);

            List<ProducerRecord<String, FilePartConsumptionState>> filePartConsumptionStates = readAllProducerRecords(
                    filePartConsumptionStateTopic, testDriver);

            Assertions
                    .assertThat(filePartConsumptionStates)
                    .hasSize(2);

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                            .map(ProducerRecord::key)
                            .collect(Collectors.toList()))
                    .contains(inputPath);

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                            .map(rec -> rec.value().getPartBaseName())
                            .collect(Collectors.toList()))
                    .contains("1", "2");

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                            .map(rec -> rec.value().isConsumed())
                            .collect(Collectors.toList()))
                    .contains(true);

            Mockito
                    .verify(mockFilePartsBatchConsumer, Mockito.times(1))
                    .accept(Mockito.anyString(), Mockito.any());
        });
    }

    @Test
    public void testOnePartMultipleBatchesMultipleParts() {
        runProcessorTest(completedBatchTopic, (testDriver, consumerRecordFactory) -> {

            final FilePartsBatch batch1 = new FilePartsBatch(
                    new FilePartInfo(
                            INPUT_FILE_PATH_1,
                            "1",
                            Instant.now().toEpochMilli(),
                            1024L),
                    false)
                    .addFilePart(
                            new FilePartInfo(
                                    INPUT_FILE_PATH_2,
                                    "2",
                                    Instant.now().toEpochMilli(),
                                    1024L))
                    .completeBatch();

            final FilePartsBatch batch2 = new FilePartsBatch(
                    new FilePartInfo(
                            INPUT_FILE_PATH_2,
                            "3",
                            Instant.now().toEpochMilli(),
                            1024L),
                    false)
                    .addFilePart(
                            new FilePartInfo(
                                    INPUT_FILE_PATH_1,
                                    "4",
                                    Instant.now().toEpochMilli(),
                                    1024L))
                    .completeBatch();

            sendMessage(testDriver, consumerRecordFactory, FEED_1, batch1);
            sendMessage(testDriver, consumerRecordFactory, FEED_2, batch2);

            List<ProducerRecord<String, FilePartConsumptionState>> filePartConsumptionStates = readAllProducerRecords(
                    filePartConsumptionStateTopic, testDriver);

            Assertions
                    .assertThat(filePartConsumptionStates)
                    .hasSize(4);

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                    .map(rec -> Tuple.of(rec.key(), rec.value().getPartBaseName()))
                            .collect(Collectors.toList()))
                    .contains(
                            Tuple.of(INPUT_FILE_PATH_1, "1"),
                            Tuple.of(INPUT_FILE_PATH_2, "2"),
                            Tuple.of(INPUT_FILE_PATH_2, "3"),
                            Tuple.of(INPUT_FILE_PATH_1, "4"));

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                            .map(ProducerRecord::key)
                            .collect(Collectors.toList()))
                    .contains(INPUT_FILE_PATH_1, INPUT_FILE_PATH_2);

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                            .map(rec -> rec.value().getPartBaseName())
                            .collect(Collectors.toList()))
                    .contains("1", "2", "3", "4");

            Assertions
                    .assertThat(filePartConsumptionStates.stream()
                            .map(rec -> rec.value().isConsumed())
                            .collect(Collectors.toList()))
                    .contains(true);

            Mockito
                    .verify(mockFilePartsBatchConsumer, Mockito.times(2))
                    .accept(Mockito.anyString(), Mockito.any());
        });
    }

    @Override
    StreamProcessor getStreamProcessor() {
        return new CompletedBatchProcessor(getStreamConfigProperties(), mockFilePartsBatchConsumer);
    }


}