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
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZMArtTopology3 {
    private static final Logger LOG = LoggerFactory.getLogger(ZMartTopology.class);
    public static void main(String[] args) throws InterruptedException {
        Properties kProps=new Properties();
        kProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "Zmarttopology3");
        kProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamsConfig = new StreamsConfig(kProps);
        Serde<String> stringSerde = Serdes.String();
        Serde<Purchase> purchaseSerde = StreamSerdes.PurchaseSerde();
        Serde<PurchasePattern> patternsSerde = StreamSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardsSerde = StreamSerdes.RewardsAcuumulatorSerde();

        StreamsBuilder builder=new StreamsBuilder();
        KStream<String,Purchase> purchaseStream=builder.stream(TopologyConstants.sourceFromTopic3, Consumed.with(stringSerde, purchaseSerde));
        purchaseStream.print(Printed.<String,Purchase>toSysOut().withLabel("PurchaseStream"));

        KStream<String,Purchase> maskedStream=purchaseStream.mapValues(p -> Purchase.builder(p).maskCreditCard().build());
        maskedStream.print(Printed.<String,Purchase>toSysOut().withLabel("MaskedStream"));
        KStream<Long,Purchase> filteredKeyedStream=maskedStream.filter((k,p)->p.getPrice()>5.00).selectKey((k, p)->p.getPurchaseDate().getTime());
        filteredKeyedStream.to(TopologyConstants.maskedCCTopic3, Produced.with(Serdes.Long(), StreamSerdes.PurchaseSerde()));
        filteredKeyedStream.print(Printed.<Long,Purchase>toSysOut().withLabel("FilteredKeyedStream"));


        KStream<String, PurchasePattern> patternsStream=maskedStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());
        patternsStream.print(Printed.<String,PurchasePattern>toSysOut().withLabel("PatternStream"));
        patternsStream.to(TopologyConstants.patternTopic3, Produced.with(stringSerde,patternsSerde));

        KStream<String, RewardAccumulator> rewardsStream=maskedStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build());
        rewardsStream.print(Printed.<String,RewardAccumulator>toSysOut().withLabel("RewardStream"));
        rewardsStream.to(TopologyConstants.rewardTopic3, Produced.with(stringSerde,rewardsSerde));

        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");
        KStream<String, Purchase>[] kstreamByDept = maskedStream.branch(isCoffee,isElectronics);
        kstreamByDept[0].to(TopologyConstants.coffee3,Produced.with(stringSerde,purchaseSerde));
        kstreamByDept[0].print(Printed.<String,Purchase>toSysOut().withLabel("Coffee"));
        kstreamByDept[1].to(TopologyConstants.electronics3,Produced.with(stringSerde,purchaseSerde));
        kstreamByDept[1].print(Printed.<String,Purchase>toSysOut().withLabel("Electronics"));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        MockDataProducer.producePurchaseData();
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Kafka Streams Application ZMArtTopology2 now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }
}
