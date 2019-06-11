package kafkastreamsevaluation.proxy.serde;

import com.esotericsoftware.kryo.pool.KryoPool;
import kafkastreamsevaluation.proxy.BatchChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchChangeEventSerde extends AbstractKafkaSerde<BatchChangeEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchChangeEventSerde.class);

//    private static final KryoFactory factory = () -> {
//        Kryo kryo = new Kryo();
//        try {
//            kryo.register(BatchChangeEvent.class);
//            kryo.register(FilePartInfo.class);
//
//            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(
//                    new StdInstantiatorStrategy());
//            kryo.setRegistrationRequired(true);
//        } catch (Exception e) {
//            LOGGER.error("Exception occurred configuring kryo instance", e);
//        }
//        return kryo;
//    };
//
//    private static final KryoPool pool = new KryoPool.Builder(factory)
//            .softReferences()
//            .build();

    public static BatchChangeEventSerde instance() {
        return new BatchChangeEventSerde();
    }

    @Override
    KryoPool getKryoPool() {
        return KryoPoolHolder.getPool();
    }

}
