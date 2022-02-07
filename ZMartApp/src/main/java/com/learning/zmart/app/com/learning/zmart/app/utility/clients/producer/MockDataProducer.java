package com.learning.zmart.app.com.learning.zmart.app.utility.clients.producer;



import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.learning.zmart.app.com.learning.zmart.app.utility.datagen.DataGenerator;
import com.learning.zmart.app.constants.TopologyConstants;
import com.learning.zmart.app.model.Purchase;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Class will produce 100 Purchase records per iteration
 * for a total of 1,000 records in one minute then it will shutdown.
 */
public class MockDataProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MockDataProducer.class);

    private static Producer<String, String> producer;
    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static Callback callback;
    private static final String TRANSACTIONS_TOPIC = TopologyConstants.sourceFromTopic5;
    private static final String YELLING_APP_TOPIC = "";
    private static final int YELLING_APP_ITERATIONS = 5;
    private static volatile boolean keepRunning = true;
    private static volatile boolean producingIQData = false;


    public static void producePurchaseData() {
        producePurchaseData(DataGenerator.DEFAULT_NUM_PURCHASES, DataGenerator.NUM_ITERATIONS, DataGenerator.NUMBER_UNIQUE_CUSTOMERS);
    }

    public static void producePurchaseData(int numberPurchases, int numberIterations, int numberCustomers) {
        Runnable generateTask = () -> {
            init();
            int counter = 0;
            while (counter++ < numberIterations  && keepRunning) {
                List<Purchase> purchases = DataGenerator.generatePurchases(numberPurchases, numberCustomers);
                List<String> jsonValues = convertToJson(purchases);
                for (String value : jsonValues) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(TRANSACTIONS_TOPIC, null, value);
                    producer.send(record, callback);
                }
                LOG.info("Record batch sent");
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            LOG.info("Done generating purchase data");

        };
        executorService.submit(generateTask);
    }

    private static List<DataGenerator.Customer> getCustomers(int numberCustomers) {
        return DataGenerator.generateCustomers(numberCustomers);
    }

    public static void produceRandomTextData() {
        Runnable generateTask = () -> {
            init();
            int counter = 0;
            while (counter++ < YELLING_APP_ITERATIONS) {
                List<String> textValues = DataGenerator.generateRandomText();

                for (String value : textValues) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(YELLING_APP_TOPIC, null, value);
                    producer.send(record, callback);
                }
                LOG.info("Text batch sent");
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            LOG.info("Done generating text data");

        };
        executorService.submit(generateTask);
    }

    public static void shutdown() {
        LOG.info("Shutting down data generation");
        keepRunning = false;

        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }

    }

    private static void init() {
        if (producer == null) {
            LOG.info("Initializing the producer");
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("acks", "1");
            properties.put("retries", "3");

            producer = new KafkaProducer<>(properties);

            callback = (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            };
            LOG.info("Producer initialized");
        }
    }

    private static <T> List<String> convertToJson(List<T> generatedDataItems) {
        List<String> jsonList = new ArrayList<>();
        for (T generatedData : generatedDataItems) {
            jsonList.add(convertToJson(generatedData));
        }
        return jsonList;
    }

    private static <T> String convertToJson(T generatedDataItem) {
        return gson.toJson(generatedDataItem);
    }
}
