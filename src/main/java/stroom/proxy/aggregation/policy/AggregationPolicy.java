package stroom.proxy.aggregation.policy;

import stroom.proxy.aggregation.model.FilePartInfo;
import stroom.proxy.aggregation.model.FilePartsBatch;

public interface AggregationPolicy {

    /**
     * @return True if the passed batch is ready for completion
     */
    boolean isBatchReady(final FilePartsBatch filePartsBatch);

    /**
     * @return True if currentBatch has capacity to accept filePartInfo
     */
    boolean canPartBeAddedToBatch(final FilePartsBatch currentBatch, final FilePartInfo filePartInfo);

    /**
     * @return False if filePartInfo would breach the policy on its own. Intended for use when creating
     * a new batch
     */
    boolean canPartBeAddedToBatch(final FilePartInfo filePartInfo);


    /**
     * @param filePartsBatch
     * @return The time in epcoh MS that the passed filePartsBatch will be deemed ready for completion
     */
    long getBatchExpiryTimeEpochMs(final FilePartsBatch filePartsBatch);
}
