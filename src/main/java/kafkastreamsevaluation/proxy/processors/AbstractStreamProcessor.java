package kafkastreamsevaluation.proxy.processors;

import kafkastreamsevaluation.util.StreamProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractStreamProcessor implements StreamProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamProcessor.class);

    private ExecutorService executorService;
    private KafkaStreams kafkaStreams;

    @Override
    public void start() {

        LOGGER.info("Starting streams processor {} with appId {}", getName(), getAppId());

        kafkaStreams = new KafkaStreams(getTopology(), getStreamConfig());

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(kafkaStreams::start);
    }

    @Override
    public void stop() {

        LOGGER.info("Stopping streams processor {} with appId {}", getName(), getAppId());

        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public abstract Topology getTopology();

    public abstract Properties getStreamConfig();
}
