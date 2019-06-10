package kafkastreamsevaluation.proxy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BatchKeySerde extends AbstractKafkaSerde<BatchKey> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchKeySerde.class);

    private static final KryoFactory factory = () -> {
        Kryo kryo = new Kryo();
        try {
            kryo.register(BatchKey.class);
            kryo.register(String.class);

            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(
                    new StdInstantiatorStrategy());
            kryo.setRegistrationRequired(true);
        } catch (Exception e) {
            LOGGER.error("Exception occurred configuring kryo instance", e);
        }
        return kryo;
    };

    private static final KryoPool pool = new KryoPool.Builder(factory)
            .softReferences()
            .build();

    @Override
    KryoPool getKryoPool() {
        return pool;
    }

    static BatchKeySerde instance() {
        return new BatchKeySerde();
    }
}
