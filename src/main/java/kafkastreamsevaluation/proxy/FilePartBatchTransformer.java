package kafkastreamsevaluation.proxy;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

class FilePartBatchTransformer implements Transformer<String, FilePartInfo, KeyValue<String, FilePartsBatch>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartBatchTransformer.class);

    private ProcessorContext processorContext;
    private String stateStoreName;
    private KeyValueStore<String, FilePartsBatch> keyValueStore;

    FilePartBatchTransformer(final String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(final ProcessorContext processorContext) {
        LOGGER.debug("Initialising transformer");
        this.processorContext = processorContext;
        this.keyValueStore = (KeyValueStore) processorContext.getStateStore(stateStoreName);

        // TODO This interval would have to be less than the max age of the batches
        // Some form of delay queue may be preferable to this somewhat crude approach
        processorContext.schedule(
                Duration.ofSeconds(30).toMillis(),
                PunctuationType.WALL_CLOCK_TIME,
                this::doPunctuate);
    }

    @Override
    public KeyValue<String, FilePartsBatch> transform(final String feedName, final FilePartInfo filePartInfo) {

        FilePartsBatch currentStoreBatch = keyValueStore.get(feedName);

        LOGGER.debug("Seen in join filePart: {}, batch size: {}", filePartInfo.getBaseName(),
                currentStoreBatch == null ? "-" : currentStoreBatch.getFilePartsCount());

        FilePartsBatch outputBatch = null;
        FilePartsBatch newStoreBatch = null;

        if (currentStoreBatch == null) {
            // no batch for this feed so create one
            newStoreBatch = new FilePartsBatch( filePartInfo, false);
            // TODO need to test for readiness after adding.

            LOGGER.debug("Created new batch, count: " + newStoreBatch.getFilePartsCount());
        } else {
            if (isBatchReady(currentStoreBatch)) {
                // completed batch can be output for onward processing
                outputBatch = currentStoreBatch.completeBatch();

                LOGGER.debug("Completing existing batch, current count: " + currentStoreBatch.getFilePartsCount());
                // TODO need to test for readiness after adding.
                newStoreBatch = new FilePartsBatch(filePartInfo, false);

                LOGGER.debug("Created new batch, count: " + newStoreBatch.getFilePartsCount());
            } else {
                newStoreBatch = currentStoreBatch.addFilePart(filePartInfo);

                LOGGER.debug("Added to existing batch, new count: " + newStoreBatch.getFilePartsCount());
            }
        }

        keyValueStore.put(feedName, newStoreBatch);

        if (outputBatch != null) {
            // send our completed batch downstream
            return new KeyValue<>(feedName, outputBatch);
        } else {
            // No complete batch so nothing goes downstream
            return null;
        }
    }

    @Override
    public void close() {
        LOGGER.debug("Closing transformer");

    }

    private void doPunctuate(long timestamp) {
        KeyValueIterator<String, FilePartsBatch> valuesIterator = keyValueStore.all();
        while (valuesIterator.hasNext()) {
            KeyValue<String, FilePartsBatch> keyValue = valuesIterator.next();
            if (keyValue.value != null) {
                FilePartsBatch completedBatch = keyValue.value.completeBatch();
                processorContext.forward(keyValue.key, completedBatch);

                keyValueStore.put(keyValue.key, null);
            }
        }
    }



    private static boolean isBatchReady(final FilePartsBatch filePartsBatch) {
        Objects.requireNonNull(filePartsBatch);
        int maxFileParts = 3;
        long maxAgeMs = Duration.ofDays(100).toMillis();
        long maxSizeBytes = 10_000;
        if (filePartsBatch.getFilePartsCount() >= maxFileParts) {
            LOGGER.debug("Part count {} has reached its limit {}", filePartsBatch.getFilePartsCount(), maxFileParts);
            return true;
        } else if (filePartsBatch.getAgeMs() >= maxAgeMs) {
            LOGGER.debug("AgeMs {} has reached its limit {}", filePartsBatch.getAgeMs(), maxAgeMs);
            return true;
        } else if (filePartsBatch.getTotalSizeBytes() >= maxSizeBytes) {
            LOGGER.debug("TotalSizeBytes {} has reached its limit {}", filePartsBatch.getTotalSizeBytes(), maxSizeBytes);
            return true;
        }
        return false;
    }
}
