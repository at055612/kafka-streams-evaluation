package kafkastreamsevaluation.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;

/**
 * NOTE: This class is only thread safe if different threads work on different feedName values.
 */
public class BatchTimeTracker implements Iterable<BatchTimeTracker.BatchTime> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchTimeTracker.class);

    private final ConcurrentSkipListSet<BatchTime> batchTimes = new ConcurrentSkipListSet<>();
    private final Map<String, BatchTime> feedToMinTimeMap = new ConcurrentHashMap<>();


    /**
     * Will replace any previous time for this feedName
     */
    void put(final String feedName, final long batchTimeMs) {
        LOGGER.debug("Put called for {}, {}", feedName, Instant.ofEpochMilli(batchTimeMs));
        BatchTime currentBatchTime = feedToMinTimeMap.get(feedName);
        if (currentBatchTime != null) {
            batchTimes.remove(currentBatchTime);
        }
        BatchTime newBatchTime = new BatchTime(feedName, batchTimeMs);
        batchTimes.add(newBatchTime);
        feedToMinTimeMap.put(feedName, newBatchTime);
    }

    OptionalLong get(String feedName) {
        BatchTime batchTime = feedToMinTimeMap.get(feedName);
        if (batchTime == null) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(batchTime.getTimeMs());
        }
    }

    void remove(final String feedName) {
        LOGGER.debug("Remove called for {}", feedName);
        BatchTime batchTime = feedToMinTimeMap.remove(feedName);
        if (batchTime != null) {
            batchTimes.remove(batchTime);
        }
    }

    NavigableSet<BatchTime> getExpiredBatches() {
        // feed name is not important here as the comparator works only on time
        BatchTime start = new BatchTime("DUMMY", 0);
        BatchTime end = new BatchTime("DUMMY", System.currentTimeMillis());

        return batchTimes.subSet(start, end);
    }

    @Override
    public Iterator<BatchTime> iterator() {
        return batchTimes.iterator();
    }

    @Override
    public void forEach(final Consumer<? super BatchTime> action) {
        batchTimes.forEach(action);
    }

    @Override
    public Spliterator<BatchTime> spliterator() {
        return batchTimes.spliterator();
    }

    class BatchTime implements Comparable<BatchTime>{

        private final String feedName;
        private final long timeMs;


        private BatchTime(final String feedName, final long timeMs) {
            this.feedName = Objects.requireNonNull(feedName);
            this.timeMs = timeMs;
        }

        @Override
        public int compareTo(final BatchTime other) {
            // oldest/smallest first
            return Long.compare(this.timeMs, other.timeMs);
        }

        String getFeedName() {
            return feedName;
        }

        long getTimeMs() {
            return timeMs;
        }

        @Override
        public String toString() {
            return "BatchTime{" +
                    "feedName='" + feedName + '\'' +
                    ", timeMs=" + timeMs +
                    '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final BatchTime that = (BatchTime) o;
            return timeMs == that.timeMs &&
                    feedName.equals(that.feedName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(feedName, timeMs);
        }
    }
}
