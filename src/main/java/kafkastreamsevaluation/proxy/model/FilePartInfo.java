package kafkastreamsevaluation.proxy.model;

import java.util.Objects;

public class FilePartInfo {

    private final FilePartRef filePartRef;
    private final long creationTimeMs;
    private final long sizeBytes;

    public FilePartInfo(final String inputFilePath,
                        final String baseName,
                        final long creationTimeMs,
                        final long sizeBytes) {


        this.filePartRef = new FilePartRef(Objects.requireNonNull(inputFilePath), Objects.requireNonNull(baseName));
        this.creationTimeMs = creationTimeMs;
        this.sizeBytes = sizeBytes;
    }

    public String getInputFilePath() {
        return filePartRef.getInputFilePath();
    }

    public String getBaseName() {
        return filePartRef.getPartBaseName();
    }

    public long getCreationTimeMs() {
        return creationTimeMs;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public FilePartRef getFilePartRef() {
        return filePartRef;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FilePartInfo that = (FilePartInfo) o;
        return creationTimeMs == that.creationTimeMs &&
                sizeBytes == that.sizeBytes &&
                filePartRef.equals(that.filePartRef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePartRef, creationTimeMs, sizeBytes);
    }

    @Override
    public String toString() {
        return "FilePartInfo{" +
                "filePartRef=" + filePartRef +
                ", creationTimeMs=" + creationTimeMs +
                ", sizeBytes=" + sizeBytes +
                '}';
    }
}
