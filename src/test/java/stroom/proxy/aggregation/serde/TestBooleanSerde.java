package stroom.proxy.aggregation.serde;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBooleanSerde {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestBooleanSerde.class);


    @Test
    public void testSerDeser_true() {
        BooleanSerde serde = new BooleanSerde();

        Boolean sourceObject = Boolean.TRUE;

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        Boolean destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }

    @Test
    public void testSerDeser_false() {
        BooleanSerde serde = new BooleanSerde();

        Boolean sourceObject = Boolean.FALSE;

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        Boolean destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }

}