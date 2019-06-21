package stroom.proxy.aggregation.serde;

import com.esotericsoftware.kryo.pool.KryoPool;
import stroom.proxy.aggregation.model.FilePartRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePartRefSerde extends AbstractKafkaSerde<FilePartRef> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartRefSerde.class);

    public static FilePartRefSerde instance() {
        return new FilePartRefSerde();
    }

    @Override
    KryoPool getKryoPool() {
        return KryoPoolHolder.getPool();
    }

}
