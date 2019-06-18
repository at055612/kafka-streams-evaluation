package kafkastreamsevaluation.proxy.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FilePartConsumptionStates {

    // partBaseName -> isConsumed
    // Initially false for each part then each part gets true when the batch it is in gets consumed,
    // i.e when it has been added to the stream store or written to a big zip for forwarding.
    private final Map<String, Boolean> consumedStates;

    public FilePartConsumptionStates() {
        consumedStates = new HashMap<>();
    }

    public FilePartConsumptionStates(final Map<String, Boolean> consumedStates) {
        this.consumedStates = Objects.requireNonNull(consumedStates);
    }

    public FilePartConsumptionStates initialise(final List<String> filePartBaseNames) {
        Map<String, Boolean> consumedStates = new HashMap<>();
        filePartBaseNames.forEach(filePartBaseName -> {
            consumedStates.put(filePartBaseName, Boolean.FALSE);
        });
        return new FilePartConsumptionStates(consumedStates);
    }

    public FilePartConsumptionStates put(final String partBaseName, final boolean isConsumed) {
        HashMap<String, Boolean> map = new HashMap<>(consumedStates);
        map.put(partBaseName, isConsumed);
        return new FilePartConsumptionStates(map);
    }

    public FilePartConsumptionStates markAsConsumed(String filePartBaseName) {
        if (!consumedStates.containsKey(filePartBaseName)) {
            throw new RuntimeException(String.format("Can't find key %s in consumedStates", filePartBaseName));
        }
        FilePartConsumptionStates filePartConsumptionStates = new FilePartConsumptionStates(consumedStates);
        filePartConsumptionStates.consumedStates.put(filePartBaseName, Boolean.TRUE);
        return filePartConsumptionStates;
    }

    public boolean haveAllBeenConsumed() {
        return consumedStates.values().stream()
                .reduce((val1, val2) -> val1 && val2)
                .orElse(false);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FilePartConsumptionStates that = (FilePartConsumptionStates) o;
        return consumedStates.equals(that.consumedStates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumedStates);
    }

    @Override
    public String toString() {
        return "FilePartConsumptionStates{" +
                "consumedStates=" + consumedStates +
                '}';
    }
}
