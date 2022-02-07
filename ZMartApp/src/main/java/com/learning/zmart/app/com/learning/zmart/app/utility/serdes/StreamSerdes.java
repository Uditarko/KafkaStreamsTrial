package com.learning.zmart.app.com.learning.zmart.app.utility.serdes;

import com.learning.zmart.app.com.learning.zmart.app.utility.serialization.JsonDeserializer;
import com.learning.zmart.app.com.learning.zmart.app.utility.serialization.JsonSerializer;
import com.learning.zmart.app.model.Purchase;
import com.learning.zmart.app.model.PurchasePattern;
import com.learning.zmart.app.model.RewardAccumulator;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StreamSerdes {

    public static Serde<Purchase> PurchaseSerde() {
        return new PurchaseSerde();
    }

    public static final class PurchaseSerde extends WrapperSerde<Purchase> {
        public PurchaseSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Purchase.class));
        }
    }

    public static Serde<PurchasePattern> PurchasePatternSerde() {
        return new PurchasePatternSerde();
    }

    public static final class PurchasePatternSerde extends WrapperSerde<PurchasePattern> {
        public PurchasePatternSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(PurchasePattern.class));
        }
    }

    public static Serde<RewardAccumulator> RewardsAcuumulatorSerde() {
        return new RewardsAcuumulatorSerde();
    }

    public static final class RewardsAcuumulatorSerde extends WrapperSerde<RewardAccumulator> {
        public RewardsAcuumulatorSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(RewardAccumulator.class));
        }
    }


    private static class WrapperSerde<T> implements Serde<T> {

        private JsonSerializer<T> serializer;
        private JsonDeserializer<T> deserializer;

        WrapperSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<T> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }
    }
}
