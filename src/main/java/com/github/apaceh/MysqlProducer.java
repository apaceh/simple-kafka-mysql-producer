package com.github.apaceh;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MysqlProducer {
    private static final String TABLE_NAME = "users";
    private static final String TOPIC = "users";

    static Logger logger = LoggerFactory.getLogger(MysqlProducer.class);
    KafkaProducer<String, String> producer;

    public static void main(String[] args) throws IOException {
        final Map<String, Long> tableMap = new HashMap<>();

        try (InputStream propertiesFile = new FileInputStream("/home/alfi/config/users-producer.properties")) {

            BinaryLogClient client = new BinaryLogClient(
                    "172.18.46.14",
                    3306,
                    "kafka",
                    "P@ssw0rd"
            );

            //create Producer properties
            Properties properties = new Properties();
            properties.load(propertiesFile);

            // create the producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            client.registerEventListener(event -> {
                EventData data = event.getData();

                if(data instanceof TableMapEventData) {
                    TableMapEventData tableData = (TableMapEventData)data;
                    tableMap.put(tableData.getTable(), tableData.getTableId());
                } else if(data instanceof WriteRowsEventData) {
                    WriteRowsEventData eventData = (WriteRowsEventData)data;
                    if(eventData.getTableId() == tableMap.get(TABLE_NAME)) {
                        for(Object[] user: eventData.getRows()) {
                            pushData(producer, getUserJson(user));
                        }
                    }
                } else if(data instanceof UpdateRowsEventData) {
                    UpdateRowsEventData eventData = (UpdateRowsEventData)data;
                    if(eventData.getTableId() == tableMap.get(TABLE_NAME)) {
                        for(Map.Entry<Serializable[], Serializable[]> row : eventData.getRows()) {
                            pushData(producer, getUserJson(row.getValue()));
                        }
                    }
                } else if(data instanceof DeleteRowsEventData) {
                    DeleteRowsEventData eventData = (DeleteRowsEventData)data;
                /*if(eventData.getTableId() == tableMap.get(TABLE_NAME)) {
                    for(Object[] product: eventData.getRows()) {
                        pusher.trigger(PRODUCT_TABLE_NAME, "delete", product[0]);
                    }
                }*/
                }
            });

            client.connect();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    static JSONObject getUserJson(Object[] user) {
        JSONObject json = new JSONObject();
        json.put("id", String.valueOf(user[0]));
        json.put("first_name", String.valueOf(user[1]));
        json.put("last_name", String.valueOf(user[2]));
        json.put("user_address", String.valueOf(user[3]));

        return json;
    }

    static void pushData(KafkaProducer<String, String> producer, JSONObject user) {
        String key = "id_" + user.getString("id");

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, key, user.toString());

        logger.info("Key: " + key);

        // send data
        producer.send(producerRecord, (recordMetadata, e) -> {
            // executes every time a record is successfully sent or an exception is thrown
            if (e == null) {
                // the record was successfully sent
                logger.info("Received new metadata. \n" +
                        "Topic: " + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
            } else {
                logger.error("Error while producing.", e);
            }
        });
    }
}


