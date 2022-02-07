package com.learning.zmart.app.transformers;

import com.learning.zmart.app.model.Purchase;
import com.learning.zmart.app.model.RewardAccumulator;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class PurchaseToRewardTransformer implements ValueTransformer<Purchase, RewardAccumulator> {

    private KeyValueStore<String, Integer> stateStore;
    private final String storeName;
    private ProcessorContext context;

    public PurchaseToRewardTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.stateStore = (KeyValueStore<String, Integer>) processorContext.getStateStore(storeName);
    }

    @Override
    public RewardAccumulator transform(Purchase purchase) {
        RewardAccumulator result = RewardAccumulator.builder(purchase).build();
        Integer prevRewardPoints = stateStore.get(result.getCustomerId());
        if(prevRewardPoints != null)
        {
            result.addRewardPoints(prevRewardPoints);
        }
        stateStore.put(result.getCustomerId(), result.getTotalRewardPoints());
        return result;
    }

    @Override
    public RewardAccumulator punctuate(long l) {
        return null;
    }

    @Override
    public void close() {

    }
}
