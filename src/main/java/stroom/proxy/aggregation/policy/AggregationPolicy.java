package stroom.proxy.aggregation.policy;

import stroom.proxy.aggregation.model.FilePartInfo;
import stroom.proxy.aggregation.model.FilePartsBatch;

public interface AggregationPolicy {

    /**
     * @return True if the passed batch is ready for completion
     */
    boolean isBatchReady(final FilePartsBatch filePartsBatch);

    /**
     * @return True if currentBatch has capacity to accept filePartInfo whether it completes it or not.
     */
    boolean canPartBeAddedToBatch(final FilePartsBatch currentBatch, final FilePartInfo filePartInfo);

    /**
     * @return True if filePartInfo would cause a batch to be completable on its own. Intended for use when creating
     * a new batch. False if the new batch would be incomplete
     */
    boolean wouldPartCompleteBatch(final FilePartInfo filePartInfo);


    /**
     * @param filePartsBatch
     * @return The time in epcoh MS that the passed filePartsBatch will be deemed ready for completion
     */
    long getBatchExpiryTimeEpochMs(final FilePartsBatch filePartsBatch);
}
