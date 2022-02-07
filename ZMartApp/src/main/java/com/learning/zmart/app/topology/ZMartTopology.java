package com.learning.zmart.app.topology;

import com.learning.zmart.app.com.learning.zmart.app.utility.clients.producer.MockDataProducer;
import com.learning.zmart.app.com.learning.zmart.app.utility.serdes.StreamSerdes;
import com.learning.zmart.app.constants.TopologyConstants;
import com.learning.zmart.app.model.Purchase;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZMartTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ZMartTopology.class);
    public static void main(String[] args) throws InterruptedException {
        Properties kProps=new Properties();
        kProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart-1st-phase");
        kProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamsConfig = new StreamsConfig(kProps);
        Serde<String> stringSerde = Serdes.String();
        Serde<Purchase> purchaseSerde = StreamSerdes.PurchaseSerde();


        StreamsBuilder builder=new StreamsBuilder();
        builder.stream(TopologyConstants.sourceFromTopic, Consumed.with(stringSerde,purchaseSerde))
                .peek((k,v) -> LOG.info("Transactions : "+k+" --> "+v))
                .mapValues(purchase ->Purchase.builder(purchase).maskCreditCard().build())
                .peek((k,v) -> LOG.info("Transactions : "+k+" --> "+v))
                .to(TopologyConstants.maskedCCTopic, Produced.with(stringSerde,purchaseSerde));

        //to produce data
        //MockDataProducer.producePurchaseData();

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }
}
