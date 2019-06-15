package kafkastreamsevaluation.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamStoreBatchConsumer implements FilePartsBatchConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamStoreBatchConsumer.class);
    /**
     * Once accept has completed all files/parts referenced in filePartsBatch
     * will be eligible for deletion so should not be used outside of this method.
     *
     * @param feedName
     * @param filePartsBatch
     */
    @Override
    public void accept(final String feedName, final FilePartsBatch filePartsBatch) {

        LOGGER.info("Writing {} {} to the stream store", feedName, filePartsBatch.getFilePartsCount());

        // TODO implement writing to stream store

    }
}
