package kafkastreamsevaluation.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class AggregationPolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationPolicy.class);

    private final long maxSizeBytes;
    private final int maxFileParts;
    private final long maxAgeMs;

    public AggregationPolicy(final long maxSizeBytes, final int maxFileParts, final long maxAgeMs) {
        this.maxSizeBytes = maxSizeBytes;
        this.maxFileParts = maxFileParts;
        this.maxAgeMs = maxAgeMs;
    }

    boolean isBatchReady(FilePartsBatch filePartsBatch) {
        Objects.requireNonNull(filePartsBatch);
        if (filePartsBatch.getFilePartsCount() >= maxFileParts) {
            LOGGER.debug("Part count {} has reached its limit {}", filePartsBatch.getFilePartsCount(), maxFileParts);
            return true;
        } else if (filePartsBatch.getAgeMs() >= maxAgeMs) {
            LOGGER.debug("AgeMs {} has reached its limit {}", filePartsBatch.getAgeMs(), maxAgeMs);
            return true;
        } else if (filePartsBatch.getTotalSizeBytes() >= maxSizeBytes) {
            LOGGER.debug("TotalSizeBytes {} has reached its limit {}", filePartsBatch.getTotalSizeBytes(), maxSizeBytes);
            return true;
        }
        return false;
    }

    boolean canPartBeAddedToBatch(final FilePartsBatch currentBatch, final FilePartInfo filePartInfo) {
        Objects.requireNonNull(currentBatch);
        Objects.requireNonNull(filePartInfo);

        long effectiveTotalSize = currentBatch.getTotalSizeBytes() + filePartInfo.getSizeBytes();
        if (effectiveTotalSize >= maxSizeBytes) {
            return false;
        }
        return true;
    }

    /**
     * @param filePartsBatch
     * @return The
     */
    long getBatchExpiryTimeEpochMs(final FilePartsBatch filePartsBatch) {
        return filePartsBatch.getMinCreationTimeMs() + maxAgeMs;
    }

//    boolean wouldBeReadyWith(final FilePartsBatch currentbatch, final FilePartInfo filePartInfo) {
//        Objects.requireNonNull(currentbatch);
//        Objects.requireNonNull(filePartInfo);
//
//        int effectivePartCount = currentbatch.getFilePartsCount() + 1;
//        if (effectivePartCount >= maxFileParts) {
//            return true;
//        }
//
//        long effectiveTotalSize = currentbatch.getTotalSizeBytes() + filePartInfo.getSizeBytes();
//        if (effectiveTotalSize >= maxSizeBytes) {
//            return true;
//        }
//
//        long filePartAgeMs = System.currentTimeMillis() - filePartInfo.getCreationTimeMs();
//        if (filePartAgeMs < 0) {
//            throw new RuntimeException(String.format("Creation time %S is in the future",
//                    Instant.ofEpochMilli(filePartInfo.getCreationTimeMs()).toString()));
//        }
//        if (filePartAgeMs > maxAgeMs || currentbatch.getAgeMs() > maxAgeMs) {
//            return true;
//        }
//        return false;
//    }



}
