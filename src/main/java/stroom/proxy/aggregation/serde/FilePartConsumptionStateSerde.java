package stroom.proxy.aggregation.serde;

import com.esotericsoftware.kryo.pool.KryoPool;
import stroom.proxy.aggregation.model.FilePartConsumptionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePartConsumptionStateSerde extends AbstractKafkaSerde<FilePartConsumptionState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartConsumptionStateSerde.class);

    public static FilePartConsumptionStateSerde instance() {
        return new FilePartConsumptionStateSerde();
    }

    @Override
    KryoPool getKryoPool() {
        return KryoPoolHolder.getPool();
    }

}
