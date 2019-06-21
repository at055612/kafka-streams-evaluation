package stroom.proxy.aggregation;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import stroom.proxy.aggregation.model.FilePartConsumptionState;
import stroom.proxy.aggregation.model.FilePartInfo;
import stroom.proxy.aggregation.model.FilePartsBatch;
import stroom.proxy.aggregation.serde.FilePartConsumptionStateSerde;
import stroom.proxy.aggregation.serde.FilePartInfoSerde;
import stroom.proxy.aggregation.serde.FilePartsBatchSerde;

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

    public static final TopicDefinition<String, FilePartInfo> FEED_TO_PARTS_TOPIC = new TopicDefinition<>(
            "FeedToParts",
            stringSerde,
            filePartInfoSerde); // feedName -> filePartInfo

    public static final TopicDefinition<String, FilePartConsumptionState> FILE_PART_CONSUMPTION_STATE_TOPIC = new TopicDefinition<>(
            "FilePartConsumedState",
            stringSerde,
            filePartConsumptionStateSerde); // inputFilePath -> partBaseName|isConsumed

    public static final TopicDefinition<String, FilePartsBatch> COMPLETED_BATCH_TOPIC = new TopicDefinition<>(
            "CompletedBatch",
            stringSerde,
            filePartsBatchSerde); // feedName -> filePartsBatch

    /*
    Run these to create all the topics with 10 partitions

    kCreateTopic() {
        topicName="$1"
        partitionCount="$2"
        if [ "${partitionCount}x" = "x" ]; then
            partitionCount=1
        fi
        echo "Creating topic [$topicName] with partition count [$partitionCount]"
        docker exec -i -t kafka bash -c "unset JMX_PORT; /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181/kafka --replication-factor 1 --partitions $partitionCount --topic $topicName"
    }
    alias kcreate='kCreateTopic'

    kcreate InputFiles 10
    kcreate FilePartConsumedState 10
    kcreate InputFileConsumedState 10
    kcreate FeedToParts 10
    kcreate CompletedBatch 10

     */

}
