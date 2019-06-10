package kafkastreamsevaluation.proxy;

import com.sun.org.apache.xerces.internal.xs.datatypes.ObjectList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

class FilePartsBatch {

    private final long minCreationTimeMs;
    private final long totalSizeBytes;
    private final List<FilePartInfo> fileParts;

    FilePartsBatch(final FilePartInfo filePartInfo) {
       this.fileParts = Collections.singletonList(Objects.requireNonNull(filePartInfo));
       this.minCreationTimeMs = filePartInfo.getCreationTimeMs();
       this.totalSizeBytes = filePartInfo.getSizeBytes();
    }

    FilePartsBatch(final List<FilePartInfo> fileParts) {
        this.fileParts = Objects.requireNonNull(fileParts);

        this.minCreationTimeMs = fileParts.stream()
                .mapToLong(FilePartInfo::getCreationTimeMs)
                .min()
                .orElse(Long.MAX_VALUE);

        this.totalSizeBytes = fileParts.stream()
                .mapToLong(FilePartInfo::getSizeBytes)
                .sum();
    }

    static FilePartsBatch emptyBatch() {
        return new FilePartsBatch(Collections.emptyList());
    }

    FilePartsBatch addFilePart(FilePartInfo filePartInfo) {
       Objects.requireNonNull(filePartInfo);

       List<FilePartInfo> newPartsList = new ArrayList<>(this.fileParts);
       newPartsList.add(filePartInfo);
       return new FilePartsBatch(newPartsList);
    }


    long getMinCreationTimeMs() {
        return minCreationTimeMs;
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
                "minCreationTimeMs=" + minCreationTimeMs +
                ", totalSizeBytes=" + totalSizeBytes +
                ", fileParts=" + fileParts +
                '}';
    }
}
