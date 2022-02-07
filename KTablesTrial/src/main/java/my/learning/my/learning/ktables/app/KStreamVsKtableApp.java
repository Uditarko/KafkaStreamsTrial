package my.learning.my.learning.ktables.app;

import my.learning.katbles.model.StockTickerData;
import my.learning.ktables.datamock.MockDataProducer;
import my.learning.ktables.serialization.serde.StreamSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

import static my.learning.ktables.datamock.MockDataProducer.STOCK_TICKER_STREAM_TOPIC;
import static my.learning.ktables.datamock.MockDataProducer.STOCK_TICKER_TABLE_TOPIC;

public class KStreamVsKtableApp {
    public static void main(String[] args) throws InterruptedException {
        StreamsConfig config = new StreamsConfig(getProperties());
        StreamsBuilder sBuilder = new StreamsBuilder();

        KTable<String, StockTickerData> tickerTable = sBuilder.table(STOCK_TICKER_TABLE_TOPIC);
        KStream<String, StockTickerData> tickerStream = sBuilder.stream(STOCK_TICKER_STREAM_TOPIC);

        tickerTable.toStream().print(Printed.<String, StockTickerData>toSysOut().withLabel("TickerTable"));
        tickerStream.print(Printed.<String, StockTickerData>toSysOut().withLabel("TickerStream"));

        int numberCompanies = 3;
        int iterations = 3;
        MockDataProducer.produceStockTickerData(numberCompanies, iterations);

        KafkaStreams kafkaStreams =  new KafkaStreams(sBuilder.build(), config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(15000);
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStreamVsKtableApp");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KStreamVsKtableApp_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KStreamVsKtableApp_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "15000");
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StreamSerdes.StockTickerDataSerde().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
