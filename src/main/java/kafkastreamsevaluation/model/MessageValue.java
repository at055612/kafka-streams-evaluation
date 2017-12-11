package kafkastreamsevaluation.model;

import org.apache.kafka.common.serialization.Serde;
import kafkastreamsevaluation.serdes.MessageValueSerde;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public interface MessageValue {

    static Serde<MessageValue> serde() {
        return new MessageValueSerde();
    }

    String toMessageString();

    ZonedDateTime getEventTime();

    Map<String, String> getAttrMap();

    Optional<String> getAttrValue(String key);

    static String toMessageString(final MessageValue messageValue) {
        StringBuilder stringBuilder = new StringBuilder()
                .append(messageValue.getEventTime().toInstant().toEpochMilli())
                .append(",");

        String attrStr = messageValue.getAttrMap().entrySet().stream()
                .map(entry -> {
                    return clean(entry.getKey()) + "=" + clean(entry.getValue());
                })
                .collect(Collectors.joining(","));

        stringBuilder.append(attrStr);
        return stringBuilder.toString();
    }

    static String clean(final String value) {
        return value
                .replace(",", "\\,")
                .replace("=", "\\=");
    }
}
