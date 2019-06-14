package kafkastreamsevaluation.proxy;

import kafkastreamsevaluation.proxy.serde.FilePartsBatchSerde;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class TestFilePartsBatchSerde {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestFilePartsBatchSerde.class);

    @Test
    public void testSerDeser() {
        FilePartsBatchSerde serde = FilePartsBatchSerde.instance();


        FilePartInfo filePartInfo1 = new FilePartInfo(
                "a/b/c/d.zip",
                "001",
                Instant.now().toEpochMilli(),
                1024L);

        FilePartInfo filePartInfo2 = new FilePartInfo(
                "a/b/c/d.zip",
                "002",
                Instant.now().toEpochMilli(),
                2048L);

        FilePartsBatch sourceObject = new FilePartsBatch(
                filePartInfo1,
                false)
                .addFilePart(filePartInfo2);

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        FilePartsBatch destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }

    @Test
    public void testSerDeser_single() {
        FilePartsBatchSerde serde = FilePartsBatchSerde.instance();


        FilePartInfo filePartInfo1 = new FilePartInfo(
                "a/b/c/d.zip",
                "001",
                Instant.now().toEpochMilli(),
                1024L);


        FilePartsBatch sourceObject = new FilePartsBatch(
                filePartInfo1,
                false);

        byte[] bytes = serde.serialize("topic", sourceObject);

        LOGGER.info("bytes length = " + bytes.length);

        FilePartsBatch destObject = serde.deserialize("topic", bytes);

        Assertions.assertThat(sourceObject)
                .isEqualTo(destObject);
    }


}