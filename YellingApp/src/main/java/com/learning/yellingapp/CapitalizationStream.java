package com.learning.yellingapp;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CapitalizationStream {
    private static final Logger LOG = LoggerFactory.getLogger(CapitalizationStream.class);
    public static void main(String[] args) {
        Properties kProps=new Properties();
        kProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        kProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamsConfig = new StreamsConfig(kProps);
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> capStream= builder.stream("YellingAppInput", Consumed.with(stringSerde, stringSerde));
        capStream.peek((key, value) -> LOG.info(key+" --> "+value)).mapValues(str -> str.toUpperCase()).to("YellingAppOutput", Produced.with(stringSerde,stringSerde));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        LOG.info("Hello World Yelling App Started");
        kafkaStreams.start();
    }

}
