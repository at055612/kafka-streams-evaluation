package stroom.proxy.aggregation;

import stroom.proxy.aggregation.model.FilePartInfo;
import org.apache.kafka.streams.KeyValue;

import java.util.List;

public interface InputFileSplitter {

    List<KeyValue<String, FilePartInfo>> split(final String inputFilePath);

}
