package kafkastreamsevaluation.proxy;

import java.util.Objects;

public class BatchKey {

    private final String feedName;
    private final long batchId;

    public BatchKey(final String feedName, final long batchId) {
        this.feedName = Objects.requireNonNull(feedName);
        this.batchId = batchId;
    }

    String getFeedName() {
        return feedName;
    }

    long getBatchId() {
        return batchId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final BatchKey batchKey = (BatchKey) o;
        return batchId == batchKey.batchId &&
                feedName.equals(batchKey.feedName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(feedName, batchId);
    }

    @Override
    public String toString() {
        return "BatchKey{" +
                "feedName='" + feedName + '\'' +
                ", batchId=" + batchId +
                '}';
    }
}
