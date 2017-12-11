package kafkastreamsevaluation.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import kafkastreamsevaluation.model.BasicMessageValue;
import kafkastreamsevaluation.model.MessageValue;

import java.util.Map;

/**
 * Wraps a basic Serde<String> to parse the string into a MessageValue object and to go back again
 */
public class MessageValueSerde implements Serde<MessageValue>, Serializer<MessageValue>, Deserializer<MessageValue> {

    private final Serde<String> stringSerde = Serdes.String();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(final String topic, final MessageValue messageValue) {
        return stringSerde.serializer().serialize(topic, messageValue != null ? messageValue.toMessageString() : null);
    }

    @Override
    public MessageValue deserialize(final String topic, final byte[] data) {
        return BasicMessageValue.fromMessageString(stringSerde.deserializer().deserialize(topic, data));
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<MessageValue> serializer() {
        return this;
    }

    @Override
    public Deserializer<MessageValue> deserializer() {
        return this;
    }
}
