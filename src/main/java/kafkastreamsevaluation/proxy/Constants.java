package kafkastreamsevaluation.proxy;

public class Constants {

    // static constants only
    private Constants() {
    }

    public static final String INPUT_FILE_TOPIC = "InputFiles"; // null -> inputFilePath
    public static final String FILE_PART_REF_COUNT_DELTAS_TOPIC = "FilePartRefCountDeltas"; // inputFilePath|baseName -> refCountDelta
    public static final String INPUT_FILE_REF_COUNT_TOPIC = "InputFileRefCount"; // inputFilePath -> refCount
    public static final String FEED_TO_PARTS_TOPIC = "FeedToParts";
    public static final String COMPLETED_BATCH_TOPIC = "CompletedBatch";
}
