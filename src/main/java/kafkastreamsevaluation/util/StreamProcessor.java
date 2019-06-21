package kafkastreamsevaluation.util;

import org.apache.kafka.streams.Topology;

import java.util.Properties;

public interface StreamProcessor {

    void start();

    void stop();

    default String getName() {
        return this.getClass().getSimpleName();
    }

    Topology getTopology();

    Properties getStreamConfig();


    default String getAppId() {
        return getName();
    }
}
