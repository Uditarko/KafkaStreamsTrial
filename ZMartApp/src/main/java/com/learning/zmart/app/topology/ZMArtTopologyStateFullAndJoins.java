package com.learning.zmart.app.topology;

import com.learning.zmart.app.com.learning.zmart.app.utility.clients.producer.MockDataProducer;
import com.learning.zmart.app.com.learning.zmart.app.utility.serdes.StreamSerdes;
import com.learning.zmart.app.constants.TopologyConstants;
import com.learning.zmart.app.extractor.TransactionTimestampExtractor;
import com.learning.zmart.app.joiners.PurchaseJoiner;
import com.learning.zmart.app.model.CorrelatedPurchase;
import com.learning.zmart.app.model.Purchase;
import com.learning.zmart.app.model.PurchasePattern;
import com.learning.zmart.app.model.RewardAccumulator;
import com.learning.zmart.app.partitioner.RewardsStreamPartitioner;
import com.learning.zmart.app.transformers.PurchaseToRewardTransformer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZMArtTopologyStateFullAndJoins {
    private static final Logger LOG = LoggerFactory.getLogger(ZMartTopology.class);

    public static void main(String[] args) throws InterruptedException {
        Properties kProps = getProperties();
        StreamsConfig streamsConfig = new StreamsConfig(kProps);


        Serde<String> stringSerde = Serdes.String();
        Serde<Purchase> purchaseSerde = StreamSerdes.PurchaseSerde();
        Serde<PurchasePattern> patternsSerde = StreamSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardsSerde = StreamSerdes.RewardsAcuumulatorSerde();

        //custom partitioner
        StreamPartitioner<String, Purchase> customPartitioner = new RewardsStreamPartitioner();
        String rewardsStoreName = "rewardPointStore";
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStoreName);
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, Serdes.Integer());


        StreamsBuilder builder = new StreamsBuilder();


        KStream<String, Purchase> purchaseStream = builder.stream(TopologyConstants.sourceFromTopic5, Consumed.with(stringSerde, purchaseSerde));
        purchaseStream.print(Printed.<String, Purchase>toSysOut().withLabel("PurchaseStream"));

        KStream<String, Purchase> maskedStream = purchaseStream.mapValues(p -> Purchase.builder(p).maskCreditCard().build());
        maskedStream.print(Printed.<String, Purchase>toSysOut().withLabel("MaskedStream"));

        KStream<String, PurchasePattern> patternsStream = maskedStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());
        patternsStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("PatternStream"));
        patternsStream.to(TopologyConstants.patternTopic5, Produced.with(stringSerde, patternsSerde));

/*        KStream<String, RewardAccumulator> rewardsStream=maskedStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build());
        rewardsStream.print(Printed.<String,RewardAccumulator>toSysOut().withLabel("RewardStream"));
        rewardsStream.to(TopologyConstants.rewardTopic4, Produced.with(stringSerde,rewardsSerde));*/
        builder.addStateStore(storeBuilder);
        KStream<String, Purchase> transformByCustomerIdAsKey = purchaseStream.through(TopologyConstants.purchaseTopicClone4,
                Produced.with(stringSerde, purchaseSerde, customPartitioner));

        KStream<String, RewardAccumulator> statefulRewardsStream = transformByCustomerIdAsKey.transformValues(() -> new PurchaseToRewardTransformer(rewardsStoreName), rewardsStoreName);
        statefulRewardsStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("StatefUlRewardsStream"));
        statefulRewardsStream.to(TopologyConstants.rewardTopic5, Produced.with(stringSerde, rewardsSerde));

        //created a keyed stream
        KStream<String, Purchase> selectKeyStream = maskedStream.selectKey((k, v) -> v.getCustomerId());


        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");
        KStream<String, Purchase>[] branchedStreams = selectKeyStream.branch(isCoffee, isElectronics);

        KStream<String, Purchase> coffeeStream = branchedStreams[0];
        KStream<String, Purchase> electronicsStream = branchedStreams[1];

        //now for joining.. identify customers who have brought coffeee and purchased electronincs within and time window of 20 mins
        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner = new PurchaseJoiner();
        JoinWindows twentyMinuteWindow = JoinWindows.of(60 * 1000 * 20);
        KStream<String, CorrelatedPurchase> joinedStream = coffeeStream.join(electronicsStream, purchaseJoiner,
                twentyMinuteWindow, Joined.with(stringSerde, purchaseSerde, purchaseSerde));
        joinedStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("JoinedStream"));
        coffeeStream.to(TopologyConstants.coffee5, Produced.with(stringSerde, purchaseSerde));
        electronicsStream.to(TopologyConstants.electronics5, Produced.with(stringSerde, purchaseSerde));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        MockDataProducer.producePurchaseData();
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Kafka Streams Application ZMArtTopology2 now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ZMArtTopologyStateFullAndJoins");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ZMArtTopologyStateFullAndJoins_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ZMArtTopologyStateFullAndJoins_clientTransactionTimestampExtractor");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
        return props;
    }
}
