package stroom.proxy.aggregation.model;

import java.util.Objects;

public class FilePartRef {

    private final String inputFilePath;
    private final String partBaseName;

    public FilePartRef(final String inputFilePath, final String partBaseName) {
        this.inputFilePath = Objects.requireNonNull(inputFilePath);
        this.partBaseName = Objects.requireNonNull(partBaseName);
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public String getPartBaseName() {
        return partBaseName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FilePartRef that = (FilePartRef) o;
        return inputFilePath.equals(that.inputFilePath) &&
                partBaseName.equals(that.partBaseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputFilePath, partBaseName);
    }

    @Override
    public String toString() {
        return "FilePartRef{" +
                "inputFilePath='" + inputFilePath + '\'' +
                ", partBaseName='" + partBaseName + '\'' +
                '}';
    }
}
