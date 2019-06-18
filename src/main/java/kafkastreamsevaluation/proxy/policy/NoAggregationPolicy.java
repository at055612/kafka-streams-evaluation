package kafkastreamsevaluation.proxy.policy;

import kafkastreamsevaluation.proxy.model.FilePartInfo;
import kafkastreamsevaluation.proxy.model.FilePartsBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class NoAggregationPolicy implements AggregationPolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(NoAggregationPolicy.class);

    private static final AggregationPolicy INSTANCE = new NoAggregationPolicy();

    /**
     * @return True if the passed batch is ready for completion
     */
    @Override
    public boolean isBatchReady(final FilePartsBatch filePartsBatch) {
        Objects.requireNonNull(filePartsBatch);
        LOGGER.debug("Part count {} has reached its limit of 1", filePartsBatch.getFilePartsCount());
        return filePartsBatch.getFilePartsCount() >= 1;
    }

    /**
     * @return True if currentBatch has capacity to accept filePartInfo
     */
    @Override
    public boolean canPartBeAddedToBatch(final FilePartsBatch currentBatch, final FilePartInfo filePartInfo) {
        Objects.requireNonNull(currentBatch);
        Objects.requireNonNull(filePartInfo);
        if (currentBatch.getFilePartsCount() == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * @return The time in epcoh MS that the passed filePartsBatch will be deemed ready for completion
     */
    @Override
    public long getBatchExpiryTimeEpochMs(final FilePartsBatch filePartsBatch) {
        // Don't care about time, the batch should be ready as soon as it is created
        return Long.MAX_VALUE;
    }

    public static AggregationPolicy getInstance() {
        return INSTANCE;
    }
}
