package kafkastreamsevaluation.proxy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class FilePartInfoSerde extends AbstractKafkaSerde<FilePartInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartInfoSerde.class);

    private static final KryoFactory factory = () -> {
        Kryo kryo = new Kryo();
        try {
            kryo.register(FilePartInfo.class);
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

    public static FilePartInfoSerde instance() {
        return new FilePartInfoSerde();
    }

    @Override
    KryoPool getKryoPool() {
        return pool;
    }

}
