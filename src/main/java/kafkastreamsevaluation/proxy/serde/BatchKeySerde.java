package kafkastreamsevaluation.proxy.serde;

import com.esotericsoftware.kryo.pool.KryoPool;
import kafkastreamsevaluation.proxy.BatchKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchKeySerde extends AbstractKafkaSerde<BatchKey> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchKeySerde.class);

    @Override
    KryoPool getKryoPool() {
        return KryoPoolHolder.getPool();
    }

    public static BatchKeySerde instance() {
        return new BatchKeySerde();
    }
}
