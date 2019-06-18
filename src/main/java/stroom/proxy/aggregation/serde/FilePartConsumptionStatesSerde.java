package stroom.proxy.aggregation.serde;

import com.esotericsoftware.kryo.pool.KryoPool;
import stroom.proxy.aggregation.model.FilePartConsumptionStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePartConsumptionStatesSerde extends AbstractKafkaSerde<FilePartConsumptionStates> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartConsumptionStatesSerde.class);

    public static FilePartConsumptionStatesSerde instance() {
        return new FilePartConsumptionStatesSerde();
    }

    @Override
    KryoPool getKryoPool() {
        return KryoPoolHolder.getPool();
    }

}
