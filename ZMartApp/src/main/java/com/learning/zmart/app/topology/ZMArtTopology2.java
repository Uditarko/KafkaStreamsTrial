package com.learning.zmart.app.topology;

import com.learning.zmart.app.com.learning.zmart.app.utility.clients.producer.MockDataProducer;
import com.learning.zmart.app.com.learning.zmart.app.utility.serdes.StreamSerdes;
import com.learning.zmart.app.constants.TopologyConstants;
import com.learning.zmart.app.model.Purchase;
import com.learning.zmart.app.model.PurchasePattern;
import com.learning.zmart.app.model.RewardAccumulator;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZMArtTopology2 {
    private static final Logger LOG = LoggerFactory.getLogger(ZMartTopology.class);
    public static void main(String[] args) throws InterruptedException {
        Properties kProps=new Properties();
        kProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "Zmarttopology2");
        kProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamsConfig = new StreamsConfig(kProps);
        Serde<String> stringSerde = Serdes.String();
        Serde<Purchase> purchaseSerde = StreamSerdes.PurchaseSerde();
        Serde<PurchasePattern> patternsSerde = StreamSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardsSerde = StreamSerdes.RewardsAcuumulatorSerde();

        StreamsBuilder builder=new StreamsBuilder();
        KStream<String,Purchase> purchaseStream=builder.stream(TopologyConstants.sourceFromTopic2, Consumed.with(stringSerde, purchaseSerde));
        purchaseStream.print(Printed.<String,Purchase>toSysOut().withLabel("PurchaseStream"));

        KStream<String,Purchase> maskedStream=purchaseStream.mapValues(p -> Purchase.builder(p).maskCreditCard().build());
        maskedStream.print(Printed.<String,Purchase>toSysOut().withLabel("MaskedStream"));

        KStream<String, PurchasePattern> patternsStream=maskedStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());
        patternsStream.print(Printed.<String,PurchasePattern>toSysOut().withLabel("PatternStream"));
        patternsStream.to(TopologyConstants.patternTopic, Produced.with(stringSerde,patternsSerde));

        KStream<String, RewardAccumulator> rewardsStream=maskedStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build());
        rewardsStream.print(Printed.<String,RewardAccumulator>toSysOut().withLabel("RewardStream"));
        rewardsStream.to(TopologyConstants.rewardTopic, Produced.with(stringSerde,rewardsSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        MockDataProducer.producePurchaseData();
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Kafka Streams Application ZMArtTopology2 now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }
}
