package kafkastreamsevaluation.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class StreamStoreBatchConsumer implements FilePartsBatchConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamStoreBatchConsumer.class);

    private final AtomicLong batchCounter = new AtomicLong();
    private final AtomicLong partCounter = new AtomicLong();
    /**
     * Once accept has completed all files/parts referenced in filePartsBatch
     * will be eligible for deletion so should not be used outside of this method.
     *
     * @param feedName
     * @param filePartsBatch
     */
    @Override
    public void accept(final String feedName, final FilePartsBatch filePartsBatch) {

        batchCounter.incrementAndGet();
        partCounter.addAndGet(filePartsBatch.getFilePartsCount());
        LOGGER.info(
                "Writing feed:{} count:{} bytes:{} age:{} to the stream store. Total batch count: {}, total part Count {}",
                feedName,
                filePartsBatch.getFilePartsCount(),
                filePartsBatch.getTotalSizeBytes(),
                filePartsBatch.getAgeMs(),
                batchCounter,
                partCounter);

        // TODO implement writing to stream store

    }
}
