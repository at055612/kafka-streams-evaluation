package stroom.proxy.aggregation;

public interface InputFileRemover {

    /**
     * Remove the file found at inputFilePath from the file system.
     */
    void remove(final String inputFilePath);
}
