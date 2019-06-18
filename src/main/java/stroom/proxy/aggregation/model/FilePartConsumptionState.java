package stroom.proxy.aggregation.model;

import java.util.Objects;

public class FilePartConsumptionState {

    private final String partBaseName;
    private final boolean isConsumed;

    public FilePartConsumptionState(final String partBaseName, final boolean isConsumed) {
        this.partBaseName = Objects.requireNonNull(partBaseName);
        this.isConsumed = isConsumed;
    }

    public String getPartBaseName() {
        return partBaseName;
    }

    public boolean isConsumed() {
        return isConsumed;
    }

    public FilePartConsumptionState markConsumed() {
        return new FilePartConsumptionState(partBaseName, true);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FilePartConsumptionState that = (FilePartConsumptionState) o;
        return isConsumed == that.isConsumed &&
                partBaseName.equals(that.partBaseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partBaseName, isConsumed);
    }

    @Override
    public String toString() {
        return "FilePartConsumtionState{" +
                "partBaseName='" + partBaseName + '\'' +
                ", isConsumed=" + isConsumed +
                '}';
    }
}
