package kafkastreamsevaluation.proxy;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestBatchKeySerde {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestBatchKeySerde.class);

    @Test
    public void testSerDeser() {
        BatchKeySerde serde = BatchKeySerde.instance();

        BatchKey sourceObject = new BatchKey("MY_FEED", 123L);

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        BatchKey destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }

}