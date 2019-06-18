package kafkastreamsevaluation.proxy.serde;

import com.esotericsoftware.kryo.pool.KryoPool;
import kafkastreamsevaluation.proxy.model.FilePartRef;
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
