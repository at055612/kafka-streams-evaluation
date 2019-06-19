package stroom.proxy.aggregation;

import stroom.proxy.aggregation.model.FilePartInfo;
import stroom.proxy.aggregation.model.FilePartsBatch;
import stroom.proxy.aggregation.policy.AggregationPolicy;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public class FilePartBatchTransformer implements Transformer<String, FilePartInfo, KeyValue<String, FilePartsBatch>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePartBatchTransformer.class);

    private final String stateStoreName;
    private final AggregationPolicySupplier aggregationPolicySupplier;
    private final BatchTimeTracker batchTimeTracker = new BatchTimeTracker();

    private ProcessorContext processorContext;
    private KeyValueStore<String, FilePartsBatch> keyValueStore;


    public FilePartBatchTransformer(final String stateStoreName,
                                    final AggregationPolicySupplier aggregationPolicySupplier) {
        this.stateStoreName = stateStoreName;
        this.aggregationPolicySupplier = aggregationPolicySupplier;
    }

    @Override
    public void init(final ProcessorContext processorContext) {
        this.processorContext = processorContext;
        this.keyValueStore = (KeyValueStore) processorContext.getStateStore(stateStoreName);
        LOGGER.debug("Initialising transformer, estimated store size: {}", keyValueStore.approximateNumEntries());
        initBatchTimeTracker();
        if (LOGGER.isDebugEnabled()) {
            dumpExpensiveStoreCount();
            dumpKeyValueStore();
//            dumpTimeTrackerContents();
        }

        // TODO This interval would have to be less than the max age of the batches
        // Some form of delay queue may be preferable to this somewhat crude approach
        processorContext.schedule(
                Duration.ofSeconds(5).toMillis(),
                PunctuationType.WALL_CLOCK_TIME,
                this::doPunctuate);
    }

    @Override
    public KeyValue<String, FilePartsBatch> transform(final String feedName, final FilePartInfo filePartInfo) {
        Objects.requireNonNull(feedName);
        Objects.requireNonNull(filePartInfo);

        FilePartsBatch currentStoreBatch = keyValueStore.get(feedName);
        final AggregationPolicy aggregationPolicy = aggregationPolicySupplier.getAggregationPolicy(feedName);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("transform called for feed: {}, filePart: {}, current batch size: {}",
                    feedName,
                    filePartInfo.getBaseName(),
                    currentStoreBatch == null ? "-" : currentStoreBatch.getFilePartsCount());
        }

        List<FilePartsBatch> completedBatches = new ArrayList<>();
        FilePartsBatch newStoreBatch;

        if (currentStoreBatch == null) {
            // no batch for this feed so create one
            newStoreBatch = createNewBatch(completedBatches, filePartInfo, aggregationPolicy);

            LOGGER.debug("Created new batch, count: " + newStoreBatch.getFilePartsCount());
        } else {
            if (aggregationPolicy.isBatchReady(currentStoreBatch)
                    || !aggregationPolicy.canPartBeAddedToBatch(currentStoreBatch, filePartInfo)) {

                // existing batch can be completed and output for onward processing
                FilePartsBatch completedBatch = currentStoreBatch.completeBatch();
                completedBatches.add(completedBatch);

                LOGGER.debug("Completing existing batch, completed batch count: " + completedBatch.getFilePartsCount());
                newStoreBatch = createNewBatch(completedBatches, filePartInfo, aggregationPolicy);

                LOGGER.debug("Created new batch, count: " + newStoreBatch.getFilePartsCount());
            } else {
                // The file part has been tested to ensure it wont blow any limits so just add it.
                newStoreBatch = currentStoreBatch.addFilePart(filePartInfo);
                LOGGER.debug("Added to existing batch, new count: " + newStoreBatch.getFilePartsCount());
            }
        }

        // Update the batch held in the store for the next message for this feedname
        if (currentStoreBatch == null && newStoreBatch == null) {
            LOGGER.debug("Both current and new batches are null, so not writing to store");
        } else {
            keyValueStore.put(feedName, newStoreBatch);
        }

        // Update the time we expect the batch to have expired on, or remove it from the tracker
//        if (newStoreBatch != null) {
//            batchTimeTracker.put(feedName, aggregationPolicy.getBatchExpiryTimeEpochMs(newStoreBatch));
//        } else {
//            batchTimeTracker.remove(feedName);
//        }

        // send our completed batch(es) (if any) downstream
        completedBatches.forEach(completedBatch -> {
                if (!completedBatch.isComplete()) {
                    throw new RuntimeException("This should not happen");
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Forwarding completed batch for feed {} with part count {}",
                            feedName, completedBatch.getFilePartsCount());
                }
                processorContext.forward(feedName, completedBatch);
        });

        // Just return null as we have sent stuff downstream manually using forward()
        return null;
    }

    /**
     * @return A new incomplete batch containing the passed filePartInfo, else return null and add a brand new
     * completed batch to completedBatches.
     */
    private FilePartsBatch createNewBatch(
            final List<FilePartsBatch> completedBatches,
            final FilePartInfo filePartInfo,
            final AggregationPolicy aggregationPolicy) {
        FilePartsBatch incompleteBatch = null;
        if (!aggregationPolicy.canPartBeAddedToBatch(filePartInfo)) {
            // make a completed batch and add to the list of completed batches
            LOGGER.debug("Creating new completed batch with part {}", filePartInfo);
            completedBatches.add(new FilePartsBatch(filePartInfo, true));
        } else {
            LOGGER.debug("Creating new incomplete batch with part {}", filePartInfo);
            incompleteBatch = new FilePartsBatch(filePartInfo, false);
        }
        return incompleteBatch;
    }

    @Override
    public void close() {
        LOGGER.debug("Closing transformer");

    }

    private void doPunctuate(long timestamp) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("doPunctuate called, estimated store size {}", keyValueStore.approximateNumEntries());
            dumpExpensiveStoreCount();
            dumpKeyValueStore();
        }

        // Loop over all the current batches in the kv store and check if any are ready to be completed
        try (KeyValueIterator<String, FilePartsBatch> allEntries = keyValueStore.all()) {
            allEntries.forEachRemaining(keyValue -> {
                if (keyValue.value != null) {
                    final String feedName = keyValue.key;
                    final FilePartsBatch currentBatch = keyValue.value;
                    final AggregationPolicy aggregationPolicy = aggregationPolicySupplier.getAggregationPolicy(feedName);
                    if (aggregationPolicy.isBatchReady(currentBatch)) {
                        final FilePartsBatch completedBatch = currentBatch.completeBatch();
                        LOGGER.debug("Completing new batch, completed batch count: " + completedBatch.getFilePartsCount());
                        keyValueStore.put(feedName, null);
                        processorContext.forward(feedName, completedBatch);
                    }
                }
            });
        }
    }

    // TODO Think we may need to make the tracker threadsafe so we can be sure it represents what is
    // in the kv store, or bin it all together and just iterate over all the batches in the store
    // testing each one, which is probably slower.
//    private void doPunctuate(long timestamp) {
//        LOGGER.debug("doPunctuate called, estimated store size {}", keyValueStore.approximateNumEntries());
//        if (LOGGER.isDebugEnabled()) {
//            dumpExpensiveStoreCount();
//            dumpKeyValueStore();
//            dumpTimeTrackerContents();
//        }
//
//        // Use our tracker of batch's expiry times (which is updated each time the batch is mutated)
//        // to get those that should have expired
//        NavigableSet<BatchTimeTracker.BatchTime> expiredBatches = batchTimeTracker.getExpiredBatches();
//
//        LOGGER.debug("Found {} expired batches", expiredBatches.size());
//
//        expiredBatches
//            .forEach(batchTime -> {
//                String feedName = batchTime.getFeedName();
//                final AggregationPolicy aggregationPolicy = aggregationPolicySupplier.getAggregationPolicy(feedName);
//                final FilePartsBatch currentBatch = keyValueStore.get(batchTime.getFeedName());
//                if (currentBatch != null) {
//                    if (aggregationPolicy.isBatchReady(currentBatch)) {
//                        FilePartsBatch completedBatch = currentBatch.completeBatch();
//                        LOGGER.debug("Completing new batch, completed batch count: " + completedBatch.getFilePartsCount());
//                        processorContext.forward(feedName, completedBatch);
//                        keyValueStore.put(feedName, null);
//                        batchTimeTracker.remove(feedName);
//                    } else {
//                        LOGGER.debug("Batch should be expired but it is not. Feed {}, tracker time {}, batch expired time {}",
//                                feedName,
//                                Instant.ofEpochMilli(batchTime.getTimeMs()),
//                                Instant.ofEpochMilli(aggregationPolicy.getBatchExpiryTimeEpochMs(currentBatch)));
//                    }
//                } else {
//                    LOGGER.debug("Batch for feed {} was in the tracker but has been removed from the store", feedName);
//                }
//            });
//    }

    /**
     * Populate the tracker by scanning over all items in the kv store
     */
    private void initBatchTimeTracker() {
        KeyValueIterator<String, FilePartsBatch> valuesIterator = keyValueStore.all();
        while (valuesIterator.hasNext()) {
            final KeyValue<String, FilePartsBatch> keyValue = valuesIterator.next();
            final String feedName = keyValue.key;
            final FilePartsBatch currentBatch = keyValue.value;
            if (currentBatch != null) {
                final AggregationPolicy aggregationPolicy = aggregationPolicySupplier.getAggregationPolicy(feedName);
                long batchExpiryTimeEpochMs = aggregationPolicy.getBatchExpiryTimeEpochMs(currentBatch);

                batchTimeTracker.put(feedName, batchExpiryTimeEpochMs);
            }
        }
    }

    private void dumpKeyValueStore() {
        StringBuilder stringBuilder = new StringBuilder();
        keyValueStore.all().forEachRemaining(keyValue -> {
            stringBuilder.append(
                    String.format("\n  %s: %s",
                            keyValue.key,
                            keyValue.value == null ? "-" : keyValue.value.getFilePartsCount()));
        });
        LOGGER.debug("Dumping keyValueStore contents{}", stringBuilder.toString());
    }

    private void dumpExpensiveStoreCount() {
        LongAdder countAll = new LongAdder();
        LongAdder countNulls = new LongAdder();
        keyValueStore.all()
                .forEachRemaining(kv -> {
                    countAll.increment();
                    if (kv.value == null) {
                        countNulls.increment();
                    }
                });
        LOGGER.debug("Store count: {}, null values {}", countAll.sum(), countNulls.sum());
    }

    private void dumpTimeTrackerContents() {
        StringBuilder stringBuilder = new StringBuilder();
        batchTimeTracker.iterator().forEachRemaining(batchTime -> {
            stringBuilder.append(
                    String.format("\n  %s: %s",
                            batchTime.getFeedName(),
                            Instant.ofEpochMilli(batchTime.getTimeMs()).toString()));
        });
        LOGGER.debug("Dumping time tracker contents{}", stringBuilder.toString());
    }

}
