package kafkastreamsevaluation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafkastreamsevaluation.model.MessageValue;

public class MessageValueTimestampExtractor implements TimestampExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageValueTimestampExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record) {
        try {
            MessageValue messageValue = (MessageValue) record.value();
            return messageValue.getEventTime().toInstant().toEpochMilli();
        } catch (Exception e) {
            LOGGER.error("Value is not the expected type %s, should be MessageValue", record.value().getClass().getName());
            return 0;
        }
    }
}
