package kafkastreamsevaluation.proxy;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Objects;

public class Constants {

    // static constants only
    private Constants() {
    }

    public static final String INPUT_FILE_TOPIC = "InputFiles"; // null -> inputFilePath
    public static final String FILE_PART_CONSUMED_STATE_TOPIC = "FilePartConsumedState"; // inputFilePath -> partBaseName|isConsumed
    public static final String FEED_TO_PARTS_TOPIC = "FeedToParts"; // feedName -> filePartInfo
    public static final String COMPLETED_BATCH_TOPIC = "CompletedBatch";

    /*
    kcreate InputFiles
    kcreate FilePartConsumedState
    kcreate InputFileConsumedState
    kcreate FeedToParts
    kcreate CompletedBatch
     */

    public static class TopicDefinition<K,V> {
        private final String name;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        public TopicDefinition(final String name, final Serde<K> keySerde, final Serde<V> valueSerde) {
            this.name = Objects.requireNonNull(name);
            this.keySerde = Objects.requireNonNull(keySerde);
            this.valueSerde = Objects.requireNonNull(valueSerde);
        }

        String getName() {
            return name;
        }

        Serde<K> getKeySerde() {
            return keySerde;
        }

        Serde<V> getValueSerde() {
            return valueSerde;
        }

        Consumed<K,V> getConsumed() {
            return Consumed.with(keySerde, valueSerde);
        }

        Produced<K,V> getProduced() {
            return Produced.with(keySerde, valueSerde);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final TopicDefinition<?, ?> that = (TopicDefinition<?, ?>) o;
            return name.equals(that.name) &&
                    keySerde.equals(that.keySerde) &&
                    valueSerde.equals(that.valueSerde);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, keySerde, valueSerde);
        }

        @Override
        public String toString() {
            return "TopicDefinition{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }
}
