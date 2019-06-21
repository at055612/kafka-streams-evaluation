package stroom.proxy.aggregation.serde;

import stroom.proxy.aggregation.model.FilePartRef;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFilePartRefSerde {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestFilePartRefSerde.class);

    @Test
    public void testSerDeser() {
        FilePartRefSerde serde = FilePartRefSerde.instance();

        FilePartRef sourceObject = new FilePartRef(
                "a/b/c/d.zip",
                "001");

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        FilePartRef destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }
}