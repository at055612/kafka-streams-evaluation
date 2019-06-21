package stroom.proxy.aggregation.processors;

import kafkastreamsevaluation.util.KafkaUtils;
import kafkastreamsevaluation.util.StreamProcessor;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.proxy.aggregation.InputFileRemover;
import stroom.proxy.aggregation.TopicDefinition;
import stroom.proxy.aggregation.Topics;
import stroom.proxy.aggregation.model.FilePartConsumptionState;
import stroom.proxy.aggregation.model.FilePartConsumptionStates;

@RunWith(MockitoJUnitRunner.class)
public class TestInputFileRemoverProcessor extends AbstractStreamProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestInputFileRemoverProcessor.class);

    private final TopicDefinition<String, FilePartConsumptionState> filePartConsumptionStateTopic = Topics.FILE_PART_CONSUMPTION_STATE_TOPIC;

    private static final String INPUT_FILE_PATH_1 = "/some/path/some/file1.zip";
    private static final String INPUT_FILE_PATH_2 = "/some/path/some/file2.zip";
    private static final String BASE_NAME_1 = "001";
    private static final String BASE_NAME_2 = "002";
    private static final String BASE_NAME_3 = "003";
    private static final String BASE_NAME_4 = "004";

    @Mock
    private InputFileRemover mockInputFileRemover;

    @Test
    public void test() {

        runProcessorTest(filePartConsumptionStateTopic, (testDriver, consumerRecordFactory) -> {

            sendConsumptionStateMsg(testDriver, consumerRecordFactory, INPUT_FILE_PATH_1, BASE_NAME_1,
                    false,
                    1,
                    0);
            sendConsumptionStateMsg(testDriver, consumerRecordFactory, INPUT_FILE_PATH_1, BASE_NAME_2,
                    false,
                    1,
                    0);
            sendConsumptionStateMsg(testDriver, consumerRecordFactory, INPUT_FILE_PATH_2, BASE_NAME_3,
                    false,
                    2,
                    0);
            sendConsumptionStateMsg(testDriver, consumerRecordFactory, INPUT_FILE_PATH_2, BASE_NAME_4,
                    false,
                    2,
                    0);

            // path 1 partially complete
            sendConsumptionStateMsg(testDriver, consumerRecordFactory, INPUT_FILE_PATH_1, BASE_NAME_1,
                    true,
                    2,
                    0);

            // path 1 fully complete, remove called, aggregate removed from store
            sendConsumptionStateMsg(testDriver, consumerRecordFactory, INPUT_FILE_PATH_1, BASE_NAME_2,
                    true,
                    1,
                    1);

            // path 2 partially complete
            sendConsumptionStateMsg(testDriver, consumerRecordFactory, INPUT_FILE_PATH_2, BASE_NAME_3,
                    true,
                    1,
                    1);

            // path 2 fully complete, remove called, aggregate removed from store
            sendConsumptionStateMsg(testDriver, consumerRecordFactory, INPUT_FILE_PATH_2, BASE_NAME_4,
                    true,
                    0,
                    2);

            Mockito
                    .verify(mockInputFileRemover, Mockito.times(1))
                    .remove(INPUT_FILE_PATH_1);

            Mockito
                    .verify(mockInputFileRemover, Mockito.times(1))
                    .remove(INPUT_FILE_PATH_2);

        });
    }

    private void sendConsumptionStateMsg(final TopologyTestDriver testDriver,
                                         final ConsumerRecordFactory<String, FilePartConsumptionState> consumerRecordFactory,
                                         final String inputFilePath,
                                         final String baseName,
                                         final boolean isConsumed,
                                         final int expectedStoreCount,
                                         final int expectedCallsToRemove) {

        sendMessage(testDriver, consumerRecordFactory, inputFilePath, new FilePartConsumptionState(baseName, isConsumed));

        KeyValueStore<String, FilePartConsumptionStates> store = testDriver.getKeyValueStore(
                InputFileRemoverProcessor.CONSUMPTION_STATES_STORE);

        KafkaUtils.dumpKeyValueStore(store);

        // if agg is null it means all were true and it has been removed from the store (tombstoned)
        // otherwise haveAllBeenConsumed should always be false
        FilePartConsumptionStates filePartConsumptionStates = store.get(inputFilePath);
        if (filePartConsumptionStates != null) {
            Assertions
                    .assertThat(filePartConsumptionStates.haveAllBeenConsumed())
                    .isFalse();
        }

        Assertions
                .assertThat(KafkaUtils.getNonNullEntryCount(store))
                .isEqualTo(expectedStoreCount);

        Mockito
                .verify(mockInputFileRemover, Mockito.times(expectedCallsToRemove))
                .remove(Mockito.anyString());
    }

    @Override
    StreamProcessor getStreamProcessor() {
        return new InputFileRemoverProcessor(getStreamConfigProperties(), mockInputFileRemover);
    }
}