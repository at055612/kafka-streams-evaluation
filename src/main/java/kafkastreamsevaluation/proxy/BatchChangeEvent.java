package kafkastreamsevaluation.proxy;

import java.util.Objects;
import java.util.Optional;

public class BatchChangeEvent {

    private final ChangeType changeType;
    private final FilePartInfo filePartInfo;

    private BatchChangeEvent(final ChangeType changeType, final FilePartInfo filePartInfo) {
        this.changeType = Objects.requireNonNull(changeType);
        switch (changeType) {
            case COMPLETE:
                if (filePartInfo != null) {
                    throw new RuntimeException("Can't have a filePartInfo when completing a batch");
                }
                break;
            case INITIALISE:
                if (filePartInfo != null) {
                    throw new RuntimeException("Can't have a filePartInfo when initialising a batch");
                }
                break;
            case ADD:
                if (filePartInfo == null) {
                    throw new RuntimeException("You must supply a filePartInfo when adding to a batch");
                }
                break;
            default:
                throw new RuntimeException(String.format("Unknown changeType %s", changeType));
        }
        this.filePartInfo = filePartInfo;
    }

    public static BatchChangeEvent createCompleteEvent() {
        return new BatchChangeEvent(ChangeType.COMPLETE, null);
    }

    static BatchChangeEvent createInitialiseEvent() {
        return new BatchChangeEvent(ChangeType.INITIALISE, null);
    }

    public static BatchChangeEvent createAddEvent(final FilePartInfo filePartInfo) {
        return new BatchChangeEvent(ChangeType.ADD, filePartInfo);
    }


    ChangeType getChangeType() {
        return changeType;
    }

    Optional<FilePartInfo> getFilePartInfo() {
        return Optional.ofNullable(filePartInfo);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final BatchChangeEvent that = (BatchChangeEvent) o;
        return changeType == that.changeType &&
                Objects.equals(filePartInfo, that.filePartInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(changeType, filePartInfo);
    }

    @Override
    public String toString() {
        return "BatchChangeEvent{" +
                "changeType=" + changeType +
                ", filePartInfo=" + filePartInfo +
                '}';
    }

    public static enum ChangeType {
        COMPLETE,
        INITIALISE,
        ADD
    }
}
