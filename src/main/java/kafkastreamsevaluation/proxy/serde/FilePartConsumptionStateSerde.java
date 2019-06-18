package kafkastreamsevaluation.proxy.serde;

import com.esotericsoftware.kryo.pool.KryoPool;
import kafkastreamsevaluation.proxy.model.FilePartConsumptionState;
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
