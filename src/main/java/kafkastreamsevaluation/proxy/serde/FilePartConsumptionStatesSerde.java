package kafkastreamsevaluation.proxy.serde;

import com.esotericsoftware.kryo.pool.KryoPool;
import kafkastreamsevaluation.proxy.FilePartConsumptionStates;
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
