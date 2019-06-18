package kafkastreamsevaluation.proxy;

import kafkastreamsevaluation.proxy.model.FilePartsBatch;

public interface FilePartsBatchConsumer {

    /**
     * Once accept has completed all files/parts referenced in filePartsBatch
     * will be eligible for deletion so should not be used outside of this method.
     */
    void accept(final String feedName, final FilePartsBatch filePartsBatch);

}
