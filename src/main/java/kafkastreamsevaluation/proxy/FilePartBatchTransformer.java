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
import java.util.ArrayList;
import java.util.List;

class FilePartBatchTransformer implements Transformer<String, FilePartInfo, KeyValue<String, FilePartsBatch>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartBatchTransformer.class);

    private final String stateStoreName;
    private final AggregationPolicySupplier aggregationPolicySupplier;

    private ProcessorContext processorContext;
    private KeyValueStore<String, FilePartsBatch> keyValueStore;


    FilePartBatchTransformer(final String stateStoreName,
                             final AggregationPolicySupplier aggregationPolicySupplier) {
        this.stateStoreName = stateStoreName;
        this.aggregationPolicySupplier = aggregationPolicySupplier;
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
        final AggregationPolicy aggregationPolicy = aggregationPolicySupplier.getAggregationPolicy(feedName);

        LOGGER.debug("transform called for feed: {}, filePart: {}, current batch size: {}",
                feedName,
                filePartInfo.getBaseName(),
                currentStoreBatch == null ? "-" : currentStoreBatch.getFilePartsCount());

        List<FilePartsBatch> completedBatches = new ArrayList<>();
        FilePartsBatch newStoreBatch;

        if (currentStoreBatch == null) {
            // no batch for this feed so create one
            newStoreBatch = new FilePartsBatch(filePartInfo, false);

            LOGGER.debug("Created new batch, count: " + newStoreBatch.getFilePartsCount());
        } else {
            if (aggregationPolicy.isBatchReady(currentStoreBatch)
                    || !aggregationPolicy.canPartBeAddedToBatch(currentStoreBatch, filePartInfo)) {

                // completed batch can be output for onward processing
                FilePartsBatch completedBatch = currentStoreBatch.completeBatch();
                completedBatches.add(completedBatch);

                LOGGER.debug("Completing existing batch, completed batch count: " + completedBatch.getFilePartsCount());
                newStoreBatch = new FilePartsBatch(filePartInfo, false);

                LOGGER.debug("Created new batch, count: " + newStoreBatch.getFilePartsCount());
            } else {
                // The part has been tested to ensure it wont blow any limits so just add it.
                newStoreBatch = currentStoreBatch.addFilePart(filePartInfo);
                LOGGER.debug("Added to existing batch, new count: " + newStoreBatch.getFilePartsCount());
            }
        }

        if (aggregationPolicy.isBatchReady(newStoreBatch)) {
            // The single part has immediately made a ready batch so complete it and put a null
            // into the store
            FilePartsBatch completedBatch = newStoreBatch.completeBatch();
            LOGGER.debug("Completing new batch, completed batch count: " + completedBatch.getFilePartsCount());
            completedBatches.add(newStoreBatch);
            LOGGER.debug("Setting store value to null");
            newStoreBatch = null;
        }

        // Update the batch held in the store for the next message for this feedname
        keyValueStore.put(feedName, newStoreBatch);

        // send our completed batch(es) (if any) downstream
        completedBatches.forEach(completedBatch ->
                processorContext.forward(feedName, completedBatch));

        // Just return null as we have sent stuff downstream manually using .forward
        return null;
    }

    @Override
    public void close() {
        LOGGER.debug("Closing transformer");

    }

    private void doPunctuate(long timestamp) {
        // TODO this seems a bit costly to keep scanning over all batches
        // Some kind of delay queue would be preferable
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

}
