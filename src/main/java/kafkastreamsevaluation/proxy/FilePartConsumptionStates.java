package kafkastreamsevaluation.proxy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilePartConsumptionStates {

    // partBaseName -> ref count
    // Initially 1 for each part then each part gets -1 when the batch it is in gets consumed,
    // i.e when it has been added to the stream store or written to a big zip for forwarding.
    private final Map<String, Boolean> consumedStates;


    public FilePartConsumptionStates(final Map<String, Boolean> consumedStates) {
        this.consumedStates = consumedStates;
    }

    FilePartConsumptionStates initialise(final List<String> filePartBaseNames) {
        Map<String, Boolean> consumedStates = new HashMap<>();
        filePartBaseNames.forEach(filePartBaseName -> {
            consumedStates.put(filePartBaseName, Boolean.FALSE);
        });
        return new FilePartConsumptionStates(consumedStates);
    }

    FilePartConsumptionStates markAsConsumed(String filePartBaseName) {
        if (!consumedStates.containsKey(filePartBaseName)) {
            throw new RuntimeException(String.format("Can't find key %s in consumedStates", filePartBaseName));
        }
        FilePartConsumptionStates filePartConsumptionStates = new FilePartConsumptionStates(consumedStates);
        filePartConsumptionStates.consumedStates.put(filePartBaseName, Boolean.TRUE);
        return filePartConsumptionStates;
    }

}
