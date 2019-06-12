package kafkastreamsevaluation.proxy.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.esotericsoftware.kryo.serializers.EnumNameSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import kafkastreamsevaluation.proxy.BatchChangeEvent;
import kafkastreamsevaluation.proxy.BatchKey;
import kafkastreamsevaluation.proxy.FilePartInfo;
import kafkastreamsevaluation.proxy.FilePartsBatch;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class KryoPoolHolder {

   private static final Logger LOGGER = LoggerFactory.getLogger(KryoPoolHolder.class);

   private static final KryoFactory factory = () -> {
      Kryo kryo = new Kryo();
      try {
         // Fixed IDs to ensure consistency in ser/deser
         // 0-8 taken by String and primitives so start at 10
         // Changing these IDs will break de-serialisation of already de-serialised data
         kryo.register(List.class, 10);
         kryo.register(ArrayList.class, 11);
         kryo.register(FilePartInfo.class, 12);
         kryo.register(FilePartsBatch.class, 13);
         kryo.register(BatchChangeEvent.class, 14);
         registerEnum(kryo, BatchChangeEvent.ChangeType.class, 15);
         kryo.register(BatchKey.class, 16);
         // custom serialises to deal with private classes
         kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer(), 17);
         kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer(), 18);

         ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(
                 new StdInstantiatorStrategy());
         kryo.setRegistrationRequired(true);
      } catch (Exception e) {
         LOGGER.error("Exception occurred configuring kryo instance", e);
      }
      return kryo;
   };

   private static void registerEnum(final Kryo kryo, final Class<? extends Enum> enumType, int id) {
      // Serialise enums using the name to allow for additions to the enum
      // More costly in bytes but safer
      kryo.register(enumType, new EnumNameSerializer(kryo, enumType), id);
   }

   private static final KryoPool pool = new KryoPool.Builder(factory)
           .softReferences()
           .build();

   static KryoPool getPool() {
      return pool;
   }

}
