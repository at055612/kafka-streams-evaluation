package kafkastreamsevaluation.proxy.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Objects;

public class BooleanSerde implements Serde<Boolean>, Serializer<Boolean>, Deserializer<Boolean> {

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public Boolean deserialize(final String topic, final byte[] data) {
        return data[0] == 0x01;
    }

    /**
     * Configure this class, which will configure the underlying serializer and deserializer.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    /**
     * Close this serde class, which will close the underlying serializer and deserializer.
     * This method has to be idempotent because it might be called multiple times.
     */
    @Override
    public void close() {

    }

    @Override
    public Serializer<Boolean> serializer() {
        return this;
    }

    @Override
    public Deserializer<Boolean> deserializer() {
        return this;
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(final String topic, final Boolean data) {
        Objects.requireNonNull(data);

        return new byte[]{(byte)(data ? 0x01 : 0x00)};
    }
}
