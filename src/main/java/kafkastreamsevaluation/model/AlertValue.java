package kafkastreamsevaluation.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class AlertValue implements MessageValue {

    public static final String KEY_ALERT_TYPE = "alertType";
    public static final String KEY_SUBJECT_PREFIX = "subject-";

    //These are the event(s) that spawned the alert
    private final List<MessageValue> subjects;
    private final MessageValue alert;
    private final Map<String, String> combinedAttrMap;

    public AlertValue(final ZonedDateTime alertTime,
            final String alertType,
                      final String description,
                      final List<MessageValue> subjects) {

        Preconditions.checkNotNull(alertType);
        Preconditions.checkNotNull(description);

        Map<String, String> alertAttrMap = ImmutableMap.of(
                BasicMessageValue.KEY_EVENT_TYPE, "ALERT",
                KEY_ALERT_TYPE, alertType,
                BasicMessageValue.KEY_DESCRIPTION, description);

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        builder.putAll(alertAttrMap);

        for (int i = 0; i < subjects.size(); i++) {
            String key = KEY_SUBJECT_PREFIX + i;
            MessageValue subject = subjects.get(i);
            String value = subject != null ? subject.toMessageString() : "";
            builder.put(key, value);
        }

        combinedAttrMap = builder.build();

        alert = new BasicMessageValue(alertTime, alertAttrMap);
        this.subjects = subjects;
        //spawn a new MessageValue for the alert and add each subject as a string to numbered subject keys
        //eg. subject1 = subject.toMessageString
        //escaping the delimiters in the message string.
    }


    static AlertValue fromMessageString(String messageValue) {

        MessageValue basicMessageValue = BasicMessageValue.fromMessageString(messageValue);

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        String alertType = basicMessageValue.getAttrValue(KEY_ALERT_TYPE).orElse("UNKNOWN_TYPE");
        String description = basicMessageValue.getAttrValue(BasicMessageValue.KEY_DESCRIPTION).orElse("");

        builder.put(KEY_ALERT_TYPE, alertType);
        builder.put(BasicMessageValue.KEY_DESCRIPTION, description);

        List<MessageValue> subjects = basicMessageValue.getAttrMap().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(KEY_SUBJECT_PREFIX))
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(entry -> BasicMessageValue.fromMessageString(entry.getValue()))
                .collect(Collectors.toList());

        return new AlertValue(basicMessageValue.getEventTime(),
                alertType,
                description,
                subjects);
    }


    @Override
    public String toMessageString() {
        return new BasicMessageValue(alert.getEventTime(), combinedAttrMap).toMessageString();
    }

    @Override
    public ZonedDateTime getEventTime() {
        return alert.getEventTime();
    }

    @Override
    public Map<String, String> getAttrMap() {
        return combinedAttrMap;
    }

    @Override
    public Optional<String> getAttrValue(String key) {
        return Optional.ofNullable(combinedAttrMap.get(key));
    }

    public List<MessageValue> getSubjects() {
        return subjects;
    }

    public String getAlertType() {
        return Optional.of(combinedAttrMap.get(KEY_ALERT_TYPE)).get();
    }

}
