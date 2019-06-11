package kafkastreamsevaluation.proxy.serde;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Map;

abstract class AbstractKafkaSerde<T> implements
    Serde<T>,
    Serializer<T>,
    Deserializer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaSerde.class);

    abstract KryoPool getKryoPool();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public byte[] serialize(final String topic, final T object) {
        return getKryoPool().run(kryo -> {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Output output = new Output(stream);
            kryo.writeClassAndObject(output, object);
            output.close();
            return stream.toByteArray();
        });
    }

    @Override
    public T deserialize(final String topic, final byte[] bytes) {
        return getKryoPool().run(kryo -> {
            Input input = new Input(bytes);
            return (T) kryo.readClassAndObject(input);
        });
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

}
