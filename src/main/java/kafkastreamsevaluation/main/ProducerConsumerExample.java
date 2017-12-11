package kafkastreamsevaluation.main;

import kafkastreamsevaluation.util.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ProducerConsumerExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConsumerExample.class);

    public static final String INPUT_TOPIC = "input";
    public static final String GROUP_ID = ProducerConsumerExample.class.getSimpleName();

    public static void main(String[] args) {


        LOGGER.info("main called with args [{}]", Arrays.stream(args).collect(Collectors.joining(" ")));
        LOGGER.info("GroupId: [{}]", GROUP_ID);

        try (KafkaProducer<String, String> kafkaProducer = KafkaUtils.getKafkaProducer()) {
            ExecutorService loggerExecutorService = KafkaUtils.startMessageLoggerConsumer(
                    GROUP_ID,
                    Collections.singletonList(INPUT_TOPIC));

            //little sleep to ensure consumer is started
//            KafkaUtils.sleep(200);


            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    INPUT_TOPIC,
                    "jblogs",
                    "Hello World");

            List<Future<RecordMetadata>> futures = KafkaUtils.sendMessages(Collections.singletonList(producerRecord));

            futures.forEach(future -> {
                try {
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
    }


}
