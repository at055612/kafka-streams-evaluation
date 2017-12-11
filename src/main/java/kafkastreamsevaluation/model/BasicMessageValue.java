package kafkastreamsevaluation.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BasicMessageValue implements MessageValue {
    public static final String KEY_EVENT_TYPE = "eventType";
    public static final String KEY_LOCATION = "location";
    public static final String KEY_IN_OUT = "in-out";
    public static final String KEY_DESCRIPTION = "description";
    private final ZonedDateTime eventTime;
    private final Map<String, String> attrMap;

    public BasicMessageValue(final ZonedDateTime eventTime,
                             final Map<String, String> attrMap) {
        this.eventTime = Preconditions.checkNotNull(eventTime);
        this.attrMap = Preconditions.checkNotNull(attrMap);
    }

    public BasicMessageValue(ZonedDateTime eventTime, String... keyValues) {
        this.eventTime = Preconditions.checkNotNull(eventTime);
        Preconditions.checkNotNull(keyValues);
        Preconditions.checkArgument(keyValues.length % 2 == 0);

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        for (int i = 0; i < keyValues.length; i = i + 2) {
            builder.put(keyValues[i], keyValues[i+1]);
        }

        this.attrMap = builder.build();
    }

    public static MessageValue fromMessageString(String messageValue) {
        if (messageValue == null || messageValue.isEmpty()) {
            return null;
        }

        Preconditions.checkNotNull(messageValue);
        String[] splits = messageValue.split(",");

        long epochMs = Long.valueOf(splits[0]);
        ZonedDateTime eventTime = ZonedDateTime.ofInstant(
                Instant.ofEpochMilli(epochMs),
                ZoneId.of(ZoneOffset.UTC.getId()));

        final Map<String, String> attrMap = new HashMap<>();
        for (int i = 1; i < splits.length; i++) {
            String split = splits[i];
            String[] kvPair = split.split("=");
            Preconditions.checkArgument(kvPair.length == 2);
            attrMap.put(kvPair[0], kvPair[1]);
        }
        return new BasicMessageValue(eventTime, attrMap);
    }

    @Override
    public String toMessageString() {
        return MessageValue.toMessageString(this);
    }

    @Override
    public ZonedDateTime getEventTime() {
        return eventTime;
    }

    @Override
    public Map<String, String> getAttrMap() {
        return attrMap;
    }

    @Override
    public Optional<String> getAttrValue(final String key) {
        return Optional.ofNullable(attrMap.get(key));
    }

    @Override
    public String toString() {
        return "MessageValue{" +
                "eventTime=" + eventTime +
                ", attrMap=" + attrMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicMessageValue that = (BasicMessageValue) o;

        if (!eventTime.equals(that.eventTime)) return false;
        return attrMap.equals(that.attrMap);
    }

    @Override
    public int hashCode() {
        int result = eventTime.hashCode();
        result = 31 * result + attrMap.hashCode();
        return result;
    }


}
