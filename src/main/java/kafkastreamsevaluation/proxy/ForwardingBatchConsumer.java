package kafkastreamsevaluation.proxy;

import kafkastreamsevaluation.proxy.model.FilePartsBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardingBatchConsumer implements FilePartsBatchConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ForwardingBatchConsumer.class);

    /**
     * Once accept has completed all files/parts referenced in filePartsBatch
     * will be eligible for deletion so should not be used outside of this method.
     *
     * @param feedName
     * @param filePartsBatch
     */
    @Override
    public void accept(final String feedName, final FilePartsBatch filePartsBatch) {

        LOGGER.info("Forwarding {} {}", feedName, filePartsBatch.getFilePartsCount());

        // TODO implement writing a zip for the batch and forwarding it to stroom/another proxy

        // TODO if we want to tee to multiple proxies then we probably want to write the batch
        // to a zip on disk, put a reference to it on a topic and have multiple consumers subscribing
        // to that topic.  We will then have the problem again of knowing when to delete that batch zip.

    }
}
