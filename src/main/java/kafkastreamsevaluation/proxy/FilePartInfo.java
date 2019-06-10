package kafkastreamsevaluation.proxy;

import java.util.Objects;

class FilePartInfo {

    private final String inputFilePath;
    private final String baseName;
    private final long creationTimeMs;
    private final long sizeBytes;

    FilePartInfo(final String inputFilePath,
                 final String baseName,
                 final long creationTimeMs,
                 final long sizeBytes) {

        this.inputFilePath = Objects.requireNonNull(inputFilePath);
        this.baseName = Objects.requireNonNull(baseName);
        this.creationTimeMs = creationTimeMs;
        this.sizeBytes = sizeBytes;
    }

    String getInputFilePath() {
        return inputFilePath;
    }

    String getBaseName() {
        return baseName;
    }

    long getCreationTimeMs() {
        return creationTimeMs;
    }

    long getSizeBytes() {
        return sizeBytes;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FilePartInfo that = (FilePartInfo) o;
        return creationTimeMs == that.creationTimeMs &&
                sizeBytes == that.sizeBytes &&
                inputFilePath.equals(that.inputFilePath) &&
                baseName.equals(that.baseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputFilePath, baseName, creationTimeMs, sizeBytes);
    }

    @Override
    public String toString() {
        return "FilePartInfo{" +
                "inputFilePath='" + inputFilePath + '\'' +
                ", baseName='" + baseName + '\'' +
                ", creationTimeMs=" + creationTimeMs +
                ", sizeBytes=" + sizeBytes +
                '}';
    }
}
