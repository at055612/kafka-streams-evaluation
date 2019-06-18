package kafkastreamsevaluation.proxy;

import kafkastreamsevaluation.proxy.policy.AggregationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class AggregationPolicySupplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationPolicySupplier.class);

    private final Map<String, AggregationPolicy> feedToAggregationPolicyMap;

    private final AggregationPolicy defaultAggregationPolicy;

    public AggregationPolicySupplier(final AggregationPolicy defaultAggregationPolicy) {
        this.defaultAggregationPolicy = defaultAggregationPolicy;
        this.feedToAggregationPolicyMap = Collections.emptyMap();
    }

    public AggregationPolicySupplier(final AggregationPolicy defaultAggregationPolicy,
                                     final Map<String, AggregationPolicy> feedToAggregationPolicyMap) {
        this.defaultAggregationPolicy = defaultAggregationPolicy;
        this.feedToAggregationPolicyMap = feedToAggregationPolicyMap;
    }

    AggregationPolicy getAggregationPolicy(final String feedName) {

        AggregationPolicy aggregationPolicy = feedToAggregationPolicyMap.get(feedName);
        if (aggregationPolicy == null) {
            aggregationPolicy = defaultAggregationPolicy;
        }

        return aggregationPolicy;
    }

}
