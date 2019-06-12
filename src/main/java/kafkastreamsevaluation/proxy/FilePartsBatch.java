package kafkastreamsevaluation.proxy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FilePartsBatch {

    private final boolean isComplete;
    private final long minCreationTimeMs;
    private final long totalSizeBytes;
    private final List<FilePartInfo> fileParts;

    FilePartsBatch(final FilePartInfo filePartInfo, final boolean isComplete) {
        this.fileParts = Collections.singletonList(Objects.requireNonNull(filePartInfo));
        this.minCreationTimeMs = filePartInfo.getCreationTimeMs();
        this.totalSizeBytes = filePartInfo.getSizeBytes();
        this.isComplete = isComplete;
    }

    FilePartsBatch(final List<FilePartInfo> fileParts, final boolean isComplete) {
        this.fileParts = new ArrayList<>(Objects.requireNonNull(fileParts));

        this.minCreationTimeMs = fileParts.stream()
                .mapToLong(FilePartInfo::getCreationTimeMs)
                .min()
                .orElse(Long.MAX_VALUE);

        this.totalSizeBytes = fileParts.stream()
                .mapToLong(FilePartInfo::getSizeBytes)
                .sum();
        this.isComplete = isComplete;
    }

    static FilePartsBatch emptyBatch() {
        return new FilePartsBatch(Collections.emptyList(), false);
    }

    FilePartsBatch addFilePart(FilePartInfo filePartInfo) {
       Objects.requireNonNull(filePartInfo);

       List<FilePartInfo> newPartsList = new ArrayList<>(this.fileParts);
       newPartsList.add(filePartInfo);
       return new FilePartsBatch(newPartsList, isComplete);
    }

    FilePartsBatch completeBatch() {
        return new FilePartsBatch(this.fileParts, true);
    }

    boolean isComplete() {
        return isComplete;
    }

    long getMinCreationTimeMs() {
        return minCreationTimeMs;
    }

    long getAgeMs() {
        return System.currentTimeMillis() - minCreationTimeMs;
    }

    long getTotalSizeBytes() {
        return totalSizeBytes;
    }

    List<FilePartInfo> getFileParts() {
        return fileParts;
    }

    int getFilePartsCount() {
        return fileParts.size();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FilePartsBatch that = (FilePartsBatch) o;
        return minCreationTimeMs == that.minCreationTimeMs &&
                totalSizeBytes == that.totalSizeBytes &&
                fileParts.equals(that.fileParts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minCreationTimeMs, totalSizeBytes, fileParts);
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
