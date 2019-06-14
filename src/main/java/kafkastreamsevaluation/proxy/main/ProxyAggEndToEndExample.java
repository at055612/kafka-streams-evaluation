package kafkastreamsevaluation.proxy.main;

import kafkastreamsevaluation.proxy.Constants;
import kafkastreamsevaluation.proxy.processors.InputFileInspector;
import kafkastreamsevaluation.proxy.serde.FilePartInfoSerde;
import kafkastreamsevaluation.util.KafkaUtils;
import kafkastreamsevaluation.util.StreamProcessor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProxyAggEndToEndExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyAggEndToEndExample.class);

    private static final String GROUP_ID_BASE = ProxyAggBatchingExampleWithTransformer.class.getSimpleName();

    private final Properties baseStreamsConfig;
    private final StreamProcessor inputFileInspector;

    public static void main(String[] args) {
        ProxyAggEndToEndExample proxyAggEndToEndExample = new ProxyAggEndToEndExample();
        proxyAggEndToEndExample.run();
    }

    ProxyAggEndToEndExample() {

        baseStreamsConfig = KafkaUtils.buildStreamsProperties();

        inputFileInspector = new InputFileInspector(baseStreamsConfig);

    }

    private void run() {

        startStreamProcessors();

        ExecutorService loggerExecutorService = KafkaUtils.startMessageLoggerConsumer(
                GROUP_ID_BASE + "_loggingConsumer",
                Arrays.asList(Constants.FEED_TO_PARTS_TOPIC),
                Serdes.String(),
                new FilePartInfoSerde());

        KafkaUtils.sleep(3_000);

        sendInputFileTestMessages();

        KafkaUtils.sleep(30_000);

        stopStreamProcessors();
    }


    private void startStreamProcessors() {

        inputFileInspector.start();

    }

    private void sendInputFileTestMessages() {
        List<Future<RecordMetadata>> futures = KafkaUtils.sendMessages(
                buildInputFileTestMessages(), Serdes.ByteArray(), Serdes.String());

        futures.forEach(future -> {
            try {
                //wait for kafka to accept the message
                RecordMetadata recordMetadata = future.get();
                LOGGER.info("Sent message - \n  topic = {}\n  partition = {}\n  offset = {}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            } catch (InterruptedException e) {
                LOGGER.error("Thread interrupted");
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<ProducerRecord<byte[], String>> buildInputFileTestMessages() {

        return IntStream.rangeClosed(1, 10)
                .mapToObj(i -> {
                    ProducerRecord<byte[], String> record = new ProducerRecord<>(
                            Constants.INPUT_FILE_TOPIC,
                            "/some/path/file_" + i);
                    return record;
                })
                .collect(Collectors.toList());
    }

    private void stopStreamProcessors() {
        inputFileInspector.stop();
    }


}
