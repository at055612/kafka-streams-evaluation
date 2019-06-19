package kafkastreamsevaluation.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.proxy.aggregation.TopicDefinition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

//TODO currently only <String,String> supported
public class KafkaUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    //TODO should be in a config file
    //Can be a sub-set of all the kafka brokers, but enough to be sure to get a connection to one
    //to get the info on the full cluster
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";
    public static final String KAFKA_ZOOKEEPER_QUORUM = "127.0.0.1:2181/kafka";

    public static KafkaProducer<String, String> buildKafkaProducer() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 5_000_000);

        Serde<String> stringSerde = Serdes.String();

        KafkaProducer<String, String> kafkaProducer = null;
        try {
            kafkaProducer = new KafkaProducer<>(
                    producerProps,
                    stringSerde.serializer(),
                    stringSerde.serializer());
        } catch (Exception e) {
            LOGGER.error("Error initialising kafka producer for bootstrap servers [{}]", KAFKA_BOOTSTRAP_SERVERS);
            throw e;
        }
        return kafkaProducer;
    }

    public static <K,V> KafkaProducer<K, V> buildKafkaProducer(final Serde<K> keySerde,
                                                               final Serde<V> valueSerde) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 5_000_000);

        KafkaProducer<K, V> kafkaProducer = null;
        try {
            kafkaProducer = new KafkaProducer<>(
                    producerProps,
                    keySerde.serializer(),
                    valueSerde.serializer());
        } catch (Exception e) {
            LOGGER.error("Error initialising kafka producer for bootstrap servers [{}]", KAFKA_BOOTSTRAP_SERVERS);
            throw e;
        }
        return kafkaProducer;
    }

    /**
     * Builds a StreamsConfig object with standard config, any additionalProps passed will be added to
     * the config object
     */
    public static StreamsConfig buildStreamsConfig(final String appId,
                                                   final Map<String, Object> additionalProps) {
        Map<String, Object> props = new HashMap<>();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
//        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, KAFKA_ZOOKEEPER_QUORUM);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);

        //latest = when the consumer starts for the first time, grab the latest offset
        //earliest = when the consumer starts for the first time, grab the earliest offset
        //latest is preferable for testing as it stops messages from previous runs from being consumed,
        //but means the consumers need to be started before anything puts new messages on the topic.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put("cache.max.bytes.buffering", 0L);

        //if multiple users are running streams on the same box they will get IO errors if they use the
        //sae state dir
        String user = Optional.ofNullable(System.getProperty("user.name")).orElse("unknownUser");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-" + user);

        //Add any additional props, overwriting any from above
        props.putAll(additionalProps);

        props.forEach((key, value) ->
                LOGGER.info("Setting Kafka Streams property {} for appId {} to [{}]", key, appId, value.toString())
        );

        return new StreamsConfig(props);
    }

    public static Properties buildStreamsProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
//        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, KAFKA_ZOOKEEPER_QUORUM);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);

        //latest = when the consumer starts for the first time, grab the latest offset
        //earliest = when the consumer starts for the first time, grab the earliest offset
        //latest is preferable for testing as it stops messages from previous runs from being consumed,
        //but means the consumers need to be started before anything puts new messages on the topic.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

//        props.put("cache.max.bytes.buffering", 0L);

        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        //if multiple users are running streams on the same box they will get IO errors if they use the
        //sae state dir
        String user = Optional.ofNullable(System.getProperty("user.name")).orElse("unknownUser");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-" + user);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }

    /**
     * Builds a StreamsConfig object with standard config, any additionalProps passed will be added to
     * the config object
     */
    public static StreamsConfig buildStreamsConfig(final String appId,
                                                   final Map.Entry<String, Object>... additionalProps) {

        Map<String, Object> props = new HashMap<>();

        if (additionalProps != null) {
            Arrays.stream(additionalProps).forEach(entry -> props.put(entry.getKey(), entry.getValue()));
        }
        return buildStreamsConfig(appId, props);
    }

    public static ExecutorService startStreamProcessor(final StreamProcessor streamProcessor,
                                                final ExecutorService executorService) {

        final KafkaStreams kafkaStreams = new KafkaStreams(
                streamProcessor.getTopology(),
                streamProcessor.getStreamConfig());

        executorService.submit(kafkaStreams::start);

        return executorService;
    }

    public static ExecutorService startStreamProcessor(final StreamProcessor streamProcessor) {

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        return startStreamProcessor(streamProcessor, executorService);
    }

    public static <K,V> List<Future<RecordMetadata>> sendMessages(
            final List<ProducerRecord<K, V>> messages,
            final Serde<K> keySerde,
            final Serde<V> valueSerde) {

        try (KafkaProducer<K, V> kafkaProducer = KafkaUtils.buildKafkaProducer(keySerde, valueSerde)) {

            List<Future<RecordMetadata>> futures = new ArrayList<>();
            messages.forEach(msg ->
                    futures.add(kafkaProducer.send(msg)));
            kafkaProducer.flush();

            return futures;
        }


    }

    public static List<Future<RecordMetadata>> sendMessages(final List<ProducerRecord<String, String>> messages) {

        try (KafkaProducer<String, String> kafkaProducer = KafkaUtils.getKafkaProducer()) {

            List<Future<RecordMetadata>> futures = new ArrayList<>();
            messages.forEach(msg -> {
                futures.add(kafkaProducer.send(msg));
            });
            kafkaProducer.flush();

            return futures;
        }
    }

    public static KafkaConsumer<String, String> buildKafkaConsumer(final String groupId,
                                                                   final boolean isAutoCommit,
                                                                   final OptionalInt autoCommitInterval) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", isAutoCommit);
        consumerProps.put("auto.commit.interval.ms", autoCommitInterval.orElse(1_000));
        consumerProps.put("session.timeout.ms", "30000");

        return new KafkaConsumer<>(
                consumerProps,
                Serdes.String().deserializer(),
                Serdes.String().deserializer());
    }

    public static <K,V> KafkaConsumer<K, V> buildKafkaConsumer(final String groupId,
                                                                   final boolean isAutoCommit,
                                                                   final OptionalInt autoCommitInterval,
                                                                   final Serde<K> keySerde,
                                                               final Serde<V> valueSerde) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", isAutoCommit);
        consumerProps.put("auto.commit.interval.ms", autoCommitInterval.orElse(1_000));
        consumerProps.put("session.timeout.ms", "30000");

        return new KafkaConsumer<>(
                consumerProps,
                keySerde.deserializer(),
                valueSerde.deserializer());
    }
    public static ExecutorService startMessagesConsumer(final KafkaConsumer<String, String> kafkaConsumer,
                                                        final Collection<String> topics,
                                                        final Consumer<ConsumerRecords<String, String>> messagesConsumer) {
        return startMessagesConsumer(kafkaConsumer,
                topics,
                messagesConsumer,
                Serdes.String(),
                Serdes.String());
    }

    /**
     * To stop consuming, call shutdownNow on the returned executorService
     * Blocks until the consumer has subscribed to the topics
     *
     * @param kafkaConsumer    A consumer for exclusive use of the thread this method creates
     * @param topics           Topics to consume from
     * @param messagesConsumer The function to consume the polled messages
     * @return The executorService for the created thread
     */
    public static <K,V> ExecutorService startMessagesConsumer(final KafkaConsumer<K, V> kafkaConsumer,
                                                        final Collection<String> topics,
                                                        final Consumer<ConsumerRecords<K, V>> messagesConsumer,
                                                        final Serde<K> keySerde,
                                                        final Serde<V> valueSerde) {

        final Semaphore subscribedSemaphore = new Semaphore(0);
        kafkaConsumer.subscribe(topics);

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.execute(() -> {

            //Subscribe to all bad event topics
            kafkaConsumer.subscribe(topics);

            //mark as subscribed
            subscribedSemaphore.release();

            try {
                boolean isFirstPoll = true;
                while (!Thread.currentThread().isInterrupted()) {
                    final ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(500));
                    if (isFirstPoll) {
                        // first successful poll so release a permit to mark the subscription as successful
                        // if it hasn't been released already
                        subscribedSemaphore.release();
                    }
                    isFirstPoll = false;
                    for (ConsumerRecord<K, V> record : records) {
                        LOGGER.trace("Received message - topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    messagesConsumer.accept(records);
                }
            } finally {
                LOGGER.info("Closing consumer for topics {}", topics);
                kafkaConsumer.close();
            }
        });

        LOGGER.info("Waiting for consumer to start on topics {}", topics);
        try {
            if (!subscribedSemaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                throw new RuntimeException("Failed to subscribe after 30s");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Thread interrupted");
            Thread.currentThread().interrupt();
        }

        LOGGER.info("Consumer started on topics {}", topics);

        return executorService;
    }

    public static <K,V> ExecutorService startMessageLoggerConsumer(final String groupId,
                                                                   final TopicDefinition<K,V> topic) {
        return startMessageLoggerConsumer(
                groupId,
                Collections.singletonList(topic.getName()),
                topic.getKeySerde(),
                topic.getValueSerde());
    }

    public static ExecutorService startMessageLoggerConsumer(final String groupId,
                                                             final Collection<String> topics) {
        return startMessageLoggerConsumer(
                buildKafkaConsumer(groupId, true, OptionalInt.empty()),
                topics);
    }

    public static <K,V> ExecutorService startMessageLoggerConsumer(final String groupId,
                                                             final Collection<String> topics,
                                                                   final Serde<K> keySerde,
                                                                   final Serde<V> valueSerde) {
        return startMessageLoggerConsumer(
                buildKafkaConsumer(groupId, true, OptionalInt.empty(), keySerde, valueSerde),
                topics,
                keySerde,
                valueSerde);
    }

    public static <K,V> ExecutorService startMessageLoggerConsumer(final KafkaConsumer<K, V> kafkaConsumer,
                                                             final Collection<String> topics,
                                                             final Serde<K> keySerde,
                                                             final Serde<V> valueSerde) {

        return startMessagesConsumer(kafkaConsumer, topics, consumerRecords -> {
                    consumerRecords.forEach(record -> {
                        LOGGER.debug("Received message - \n  topic = {}\n  partition = {}\n  offset = {}\n  key = {}\n  value = {}",
                                record.topic(), record.partition(), record.offset(), record.key().toString(), record.value().toString());
                    });
                },
                keySerde,
                valueSerde);
    }

    public static ExecutorService startMessageLoggerConsumer(final KafkaConsumer<String, String> kafkaConsumer,
                                                             final Collection<String> topics) {

        return startMessagesConsumer(kafkaConsumer, topics, consumerRecords -> {
            consumerRecords.forEach(record -> {
                LOGGER.debug("Received message - \n  topic = {}\n  partition = {}\n  offset = {}\n  key = {}\n  value = {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            });
        });
    }

    public static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            LOGGER.error("Thread interrupted");
            Thread.currentThread().interrupt();
        }
    }


    public static Thread.UncaughtExceptionHandler buildUncaughtExceptionHandler(final String appId) {
        return (t, e) ->
                LOGGER.error("Uncaught exception in stream processor with appId {} in thread {}",
                        appId,
                        t.getName(),
                        e);
    }

    public static KafkaProducer<String, String> getKafkaProducer() {
        return ProducerHolder.kafkaProducer;
    }

    public static  Predicate<String, String> buildAlwaysTrueStreamPeeker(final String appId) {

        return buildAlwaysTrueStreamPeeker(appId, String.class, String.class);
    }

    public static <K,V> Predicate<K, V> buildAlwaysTrueStreamPeeker(final String appId,
                                                                    final Class<K> keyType,
                                                                    final Class<V> valueType) {
        return (k, v) -> {
            LOGGER.debug("Seen message in stream - \n  appId = {}\n  key = {}\n  value = {}",
                    appId, k.toString(), v.toString());
            //abuse of a predicate as a peek method on the stream, so always return true so the
            //steam is not mutated
            return true;
        };
    }

    public static <K,V> ForeachAction<K, V> buildLoggingStreamPeeker(final String appId,
                                                                     final Class<K> keyType,
                                                                     final Class<V> valueType) {
        return (k, v) -> {
            LOGGER.debug("Seen message in stream - \n  appId = {}\n  key = {}\n  value = {}",
                    appId,
                    k == null ? "NULL" : k.toString(),
                    v == null ? "NULL" : v.toString());
            //abuse of a predicate as a peek method on the stream, so always return true so the
            //steam is not mutated
        };
    }

    private static class ProducerHolder {
        //shared instance as producer is thread safe
        private static KafkaProducer<String, String> kafkaProducer = buildKafkaProducer();

    }




}
