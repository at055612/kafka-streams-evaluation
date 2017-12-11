package kafkastreamsevaluation;

import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafkastreamsevaluation.model.BasicMessageValue;
import kafkastreamsevaluation.model.MessageValue;

import java.time.ZonedDateTime;
import java.util.Map;

public class TestMessageValue {


    private static final Logger LOGGER = LoggerFactory.getLogger(TestMessageValue.class);


    @Test
    public void toMessageStringAndBack() throws Exception {
        ZonedDateTime eventTime = ZonedDateTime.now();
        Map<String, String> attrMap = ImmutableMap.of(
                "eventType", "type1",
                "location", "location1",
                "description", "This is some text");

        BasicMessageValue messageValue = new BasicMessageValue(eventTime, attrMap);

        LOGGER.info(messageValue.toString());
        LOGGER.info(messageValue.toMessageString());

        String messageValueStr = messageValue.toMessageString();

        MessageValue messageValue2 = BasicMessageValue.fromMessageString(messageValueStr);

        Assertions.assertThat(messageValue.getEventTime())
                .isEqualTo(messageValue2.getEventTime());

        Assertions.assertThat(messageValue.getAttrMap())
                .containsAllEntriesOf(messageValue2.getAttrMap());
    }

    @Test
    public void toMessageStringAndBack2() throws Exception {
        ZonedDateTime eventTime = ZonedDateTime.now();

        BasicMessageValue messageValue = new BasicMessageValue(eventTime,
                "eventType", "type1",
                "location", "location1",
                "description", "This is some text");

        LOGGER.info(messageValue.toString());
        LOGGER.info(messageValue.toMessageString());

        String messageValueStr = messageValue.toMessageString();

        MessageValue messageValue2 = BasicMessageValue.fromMessageString(messageValueStr);

        Assertions.assertThat(messageValue.getEventTime())
                .isEqualTo(messageValue2.getEventTime());

        Assertions.assertThat(messageValue.getAttrMap())
                .containsAllEntriesOf(messageValue2.getAttrMap());
    }
}