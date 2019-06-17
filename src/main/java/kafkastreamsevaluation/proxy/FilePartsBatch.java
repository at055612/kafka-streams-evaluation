package kafkastreamsevaluation.proxy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FilePartsBatch {

    private final boolean isComplete;
    private final long minCreationTimeMs;
    private final long totalSizeBytes;
    private final List<FilePartRef> fileParts;

    private FilePartsBatch(final boolean isComplete,
                           final long minCreationTimeMs,
                           final long totalSizeBytes,
                           final List<FilePartRef> fileParts) {
        this.isComplete = isComplete;
        this.minCreationTimeMs = minCreationTimeMs;
        this.totalSizeBytes = totalSizeBytes;
        this.fileParts = fileParts;
    }

    public FilePartsBatch(final FilePartInfo filePartInfo,
                          final boolean isComplete) {
        this.fileParts = Collections.singletonList(Objects.requireNonNull(filePartInfo.getFilePartRef()));
        this.minCreationTimeMs = filePartInfo.getCreationTimeMs();
        this.totalSizeBytes = filePartInfo.getSizeBytes();
        this.isComplete = isComplete;
    }

//    FilePartsBatch(final List<FilePartInfo> fileParts,
//                   final boolean isComplete) {
//        this.fileParts = new ArrayList<>(Objects.requireNonNull(fileParts));
//
//        this.minCreationTimeMs = fileParts.stream()
//                .mapToLong(FilePartInfo::getCreationTimeMs)
//                .min()
//                .orElse(Long.MAX_VALUE);
//
//        this.totalSizeBytes = fileParts.stream()
//                .mapToLong(FilePartInfo::getSizeBytes)
//                .sum();
//        this.isComplete = isComplete;
//    }

    public FilePartsBatch addFilePart(FilePartInfo filePartInfo) {
       Objects.requireNonNull(filePartInfo);

       final List<FilePartRef> newPartsList = new ArrayList<>(this.fileParts);
       newPartsList.add(filePartInfo.getFilePartRef());

       // compute the new aggregates
       long newMinCreationTimeMs = Math.min(this.minCreationTimeMs, filePartInfo.getCreationTimeMs());
       long newTotalSizeBytes = this.totalSizeBytes + filePartInfo.getSizeBytes();

       return new FilePartsBatch(isComplete, newMinCreationTimeMs, newTotalSizeBytes, newPartsList);
    }

    public FilePartsBatch completeBatch() {
        return new FilePartsBatch(true, minCreationTimeMs, totalSizeBytes, fileParts);
    }

    public boolean isComplete() {
        return isComplete;
    }

    long getMinCreationTimeMs() {
        return minCreationTimeMs;
    }

    public long getAgeMs() {
        return System.currentTimeMillis() - minCreationTimeMs;
    }

    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }

    public List<FilePartRef> getFileParts() {
        return fileParts;
    }

    public int getFilePartsCount() {
        return fileParts.size();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FilePartsBatch that = (FilePartsBatch) o;
        return isComplete == that.isComplete &&
                minCreationTimeMs == that.minCreationTimeMs &&
                totalSizeBytes == that.totalSizeBytes &&
                fileParts.equals(that.fileParts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isComplete, minCreationTimeMs, totalSizeBytes, fileParts);
    }

    @Override
    public String toString() {
        return "FilePartsBatch{" +
                "isComplete=" + isComplete +
                ", minCreationTimeMs=" + minCreationTimeMs +
                ", totalSizeBytes=" + totalSizeBytes +
                ", fileParts=" + fileParts +
                '}';
    }
}
