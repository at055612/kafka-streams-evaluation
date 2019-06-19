package stroom.proxy.aggregation.processors;

import kafkastreamsevaluation.util.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import stroom.proxy.aggregation.TopicDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;

public abstract class AbstractStreamProcessorTest {

    <K,V> ConsumerRecordFactory<K,V> getConsumerRecordFactory(final TopicDefinition<K, V> topicDefinition) {
        return new ConsumerRecordFactory<>(
                topicDefinition.getName(),
                topicDefinition.getKeySerde().serializer(),
                topicDefinition.getValueSerde().serializer());
    }

    <K,V> Optional<ProducerRecord<K,V>> readProducerRecord(final TopicDefinition<K,V> topicDefinition,
                                                                   final TopologyTestDriver testDriver) {

        return Optional.ofNullable(testDriver.readOutput(
                topicDefinition.getName(),
                topicDefinition.getKeySerde().deserializer(),
                topicDefinition.getValueSerde().deserializer()));
    }

    <K,V> List<ProducerRecord<K,V>> readAllProducerRecords(final TopicDefinition<K, V> topicDefinition,
                                                           final TopologyTestDriver testDriver) {

        ProducerRecord<K,V> producerRecord = null;
        List<ProducerRecord<K,V>> producerRecords = new ArrayList<>();

        do {
            producerRecord = testDriver.readOutput(
                    topicDefinition.getName(),
                    topicDefinition.getKeySerde().deserializer(),
                    topicDefinition.getValueSerde().deserializer());

            if (producerRecord != null) {
                producerRecords.add(producerRecord);
            }
        } while (producerRecord != null);
        return producerRecords;
    }

    <K,V> void sendMessage(final TopologyTestDriver testDriver,
                           final ConsumerRecordFactory<K,V> consumerRecordFactory,
                           final K key,
                           final V value) {

        ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecordFactory.create(
                key,
                value);

        testDriver.pipeInput(consumerRecord);
    }

    <K,V> void sendMessages(final TopologyTestDriver testDriver,
                           final ConsumerRecordFactory<K,V> consumerRecordFactory,
                           final List<KeyValue<K,V>> keyValues) {

        keyValues.forEach(keyValue -> {
            ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecordFactory.create(
                    keyValue.key,
                    keyValue.value);

            testDriver.pipeInput(consumerRecord);
        });
    }

    <K,V> KeyValue<K,V> convertKeyValue(final TopicDefinition<K,V> topicDefinition,
                                                final byte[] keyBytes,
                                                byte[] valueBytes) {
        K key = keyBytes == null ? null : topicDefinition.getKeySerde()
                .deserializer()
                .deserialize(topicDefinition.getName(), keyBytes);

        V value = valueBytes == null ? null : topicDefinition.getValueSerde()
                .deserializer()
                .deserialize(topicDefinition.getName(), valueBytes);

        return new KeyValue<>(key, value);
    }

    protected Properties getStreamConfigProperties() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "not_used_but_required:9999");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);
        props.put("cache.max.bytes.buffering", 1024L);
        return props;
    }

    <K,V> void runProcessorTest(final TopicDefinition<K,V> inputTopicDefinition,
                                final BiConsumer<TopologyTestDriver, ConsumerRecordFactory<K,V>> testAction) {

        StreamProcessor streamProcessor = getStreamProcessor();

        TopologyTestDriver testDriver = new TopologyTestDriver(
                streamProcessor.getTopology(),
                streamProcessor.getStreamConfig());

        ConsumerRecordFactory<K,V> factory = getConsumerRecordFactory(inputTopicDefinition);

        // perform the test
        testAction.accept(testDriver, factory);
    }

    abstract StreamProcessor getStreamProcessor();
}
