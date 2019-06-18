package kafkastreamsevaluation.proxy;

import kafkastreamsevaluation.proxy.serde.FilePartConsumptionStateSerde;
import kafkastreamsevaluation.proxy.serde.FilePartInfoSerde;
import kafkastreamsevaluation.proxy.serde.FilePartsBatchSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class Topics {

    // Only static methods/members
    private Topics() {
    }

    // Reusing these assumes they are all stateless or threadsafe
    private static Serde<String> stringSerde = Serdes.String();
    private static Serde<byte[]> byteArraySerde = Serdes.ByteArray();
    private static Serde<FilePartInfo> filePartInfoSerde = FilePartInfoSerde.instance();
    private static Serde<FilePartsBatch> filePartsBatchSerde = FilePartsBatchSerde.instance();
    private static Serde<FilePartConsumptionState> filePartConsumptionStateSerde = FilePartConsumptionStateSerde.instance();

    public static final TopicDefinition<byte[], String> INPUT_FILE_TOPIC = new TopicDefinition<>(
            "InputFiles",
            byteArraySerde,
            stringSerde); // null -> inputFilePath

    public static final TopicDefinition<String, FilePartConsumptionState> FILE_PART_CONSUMPTION_STATE_TOPIC = new TopicDefinition<>(
            "FilePartConsumedState",
            stringSerde,
            filePartConsumptionStateSerde); // inputFilePath -> partBaseName|isConsumed

    public static final TopicDefinition<String, FilePartInfo> FEED_TO_PARTS_TOPIC = new TopicDefinition<>(
            "FeedToParts",
            stringSerde,
            filePartInfoSerde); // feedName -> filePartInfo

    public static final TopicDefinition<String, FilePartsBatch> COMPLETED_BATCH_TOPIC = new TopicDefinition<>(
            "CompletedBatch",
            stringSerde,
            filePartsBatchSerde); // feedName -> filePartsBatch

    /*
    Run these to create all the topics

    kcreate InputFiles
    kcreate FilePartConsumedState
    kcreate InputFileConsumedState
    kcreate FeedToParts
    kcreate CompletedBatch
     */

}
