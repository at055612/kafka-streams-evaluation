package stroom.proxy.aggregation;

import stroom.proxy.aggregation.model.FilePartsBatch;
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

        LOGGER.info(
                "Writing feed:{} count:{} bytes:{} age:{} to the stream store.",
                feedName,
                filePartsBatch.getFilePartsCount(),
                filePartsBatch.getTotalSizeBytes(),
                filePartsBatch.getAgeMs());

        // TODO implement writing to stream store

    }
}
