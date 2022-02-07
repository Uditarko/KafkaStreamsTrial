package my.learning.ktables.serialization.serde;


import my.learning.katbles.model.ShareVolume;
import my.learning.katbles.model.StockTickerData;
import my.learning.katbles.model.StockTransaction;
import my.learning.ktables.serialization.JsonDeserializer;
import my.learning.ktables.serialization.JsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StreamSerdes {

    public static Serde<StockTransaction> StockTransactionSerde() {
        return new StockTransactionSerde();
    }

    public static final class StockTransactionSerde extends WrapperSerde<StockTransaction> {
        public StockTransactionSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(StockTransaction.class));
        }
    }


    public static Serde<ShareVolume> ShareVolumeSerde() {
        return new ShareVolumeSerde();
    }

    public static final class ShareVolumeSerde extends WrapperSerde<ShareVolume> {
        public ShareVolumeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(ShareVolume.class));
        }
    }


    public static Serde<StockTickerData> StockTickerDataSerde() {
        return new StockTickerDataSerde();
    }

    public static final class StockTickerDataSerde extends WrapperSerde<StockTickerData> {
        public StockTickerDataSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(StockTickerData.class));
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
