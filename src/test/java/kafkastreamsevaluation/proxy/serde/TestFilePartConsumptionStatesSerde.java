package kafkastreamsevaluation.proxy.serde;

import kafkastreamsevaluation.proxy.model.FilePartConsumptionStates;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class TestFilePartConsumptionStatesSerde {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestFilePartConsumptionStatesSerde.class);

    @Test
    public void testSerDeser() {
        FilePartConsumptionStatesSerde serde = new FilePartConsumptionStatesSerde();

        HashMap<String, Boolean> map = new HashMap<>();
        map.put("a", false);
        map.put("b", true);
        map.put("c", false);
        map.put("d", true);

        FilePartConsumptionStates sourceObject = new FilePartConsumptionStates(map);


        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        FilePartConsumptionStates destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }
}