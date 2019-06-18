package kafkastreamsevaluation.proxy;

public class Constants {

    // static constants only
    private Constants() {
    }

    public static final String INPUT_FILE_TOPIC = "InputFiles"; // null -> inputFilePath
    public static final String FILE_PART_CONSUMED_STATE_TOPIC = "FilePartConsumedState"; // inputFilePath -> partBaseName|isConsumed
    public static final String INPUT_FILE_CONSUMED_STATE_TOPIC = "InputFileConsumedState"; // inputFilePath -> true/false
    public static final String FEED_TO_PARTS_TOPIC = "FeedToParts";
    public static final String COMPLETED_BATCH_TOPIC = "CompletedBatch";

    /*
    kcreate InputFiles
    kcreate FilePartConsumedState
    kcreate InputFileConsumedState
    kcreate FeedToParts
    kcreate CompletedBatch
     */

}
