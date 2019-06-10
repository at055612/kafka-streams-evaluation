package kafkastreamsevaluation.proxy;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.junit.Assert.*;

public class TestFilePartInfoSerde {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestFilePartInfoSerde.class);

    @Test
    public void testSerDeser() {
        FilePartInfoSerde serde = FilePartInfoSerde.instance();

        FilePartInfo sourceObject = new FilePartInfo(
                "a/b/c/d.zip",
                "001",
                Instant.now().toEpochMilli(),
                1024L);

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        FilePartInfo destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }
}