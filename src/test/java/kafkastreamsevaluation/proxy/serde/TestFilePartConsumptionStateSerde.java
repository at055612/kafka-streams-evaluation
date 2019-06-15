package kafkastreamsevaluation.proxy.serde;

import kafkastreamsevaluation.proxy.FilePartConsumptionState;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFilePartConsumptionStateSerde {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestFilePartConsumptionStateSerde.class);

    @Test
    public void testSerDeser() {
        FilePartConsumptionStateSerde serde = FilePartConsumptionStateSerde.instance();

        FilePartConsumptionState sourceObject = new FilePartConsumptionState("001", false);

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        FilePartConsumptionState destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }
}