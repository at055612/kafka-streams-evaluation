package kafkastreamsevaluation.proxy.policy;

import kafkastreamsevaluation.proxy.model.FilePartInfo;
import kafkastreamsevaluation.proxy.model.FilePartsBatch;

public interface AggregationPolicy {

    /**
     * @return True if the passed batch is ready for completion
     */
    boolean isBatchReady(FilePartsBatch filePartsBatch);

    /**
     * @return True if currentBatch has capacity to accept filePartInfo
     */
    boolean canPartBeAddedToBatch(FilePartsBatch currentBatch, FilePartInfo filePartInfo);


    /**
     * @param filePartsBatch
     * @return The time in epcoh MS that the passed filePartsBatch will be deemed ready for completion
     */
    long getBatchExpiryTimeEpochMs(FilePartsBatch filePartsBatch);
}
