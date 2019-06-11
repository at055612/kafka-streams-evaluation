package kafkastreamsevaluation.proxy;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;


public class TestBatchChangeEventSerde {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestBatchChangeEventSerde.class);

    @Test
    public void testSerDeser_completionEvent() {
        BatchChangeEventSerde serde = BatchChangeEventSerde.instance();

        BatchChangeEvent sourceObject = BatchChangeEvent.createCompleteEvent();

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        BatchChangeEvent destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }

    @Test
    public void testSerDeser_addEvent() {
        BatchChangeEventSerde serde = BatchChangeEventSerde.instance();

        FilePartInfo filePartInfo = new FilePartInfo(
                "/a/b/c/d.zip",
                "001",
                Instant.now().toEpochMilli(),
                1024L);
        BatchChangeEvent sourceObject = BatchChangeEvent.createAddEvent(filePartInfo);

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        BatchChangeEvent destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }

}