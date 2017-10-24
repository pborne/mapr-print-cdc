package com.mapr.samples.db.cdc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ojai.FieldPath;
import org.ojai.KeyValue;
import org.ojai.store.cdc.ChangeDataRecord;
import org.ojai.store.cdc.ChangeDataRecordType;
import org.ojai.store.cdc.ChangeNode;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class PrintCDC {

    private static String CHANGE_LOG = "/user/mapr/changelog:product_sales_tbl";

    public static void main(String[] args) {

        // Consumer configuration
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("group.id", "cdc.product_sales_tbl");
        consumerProperties.setProperty("enable.auto.commit", "true");
        consumerProperties.setProperty("auto.offset.reset", "latest");
        consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.setProperty("value.deserializer", "com.mapr.db.cdc.ChangeDataRecordDeserializer");

        // Consumer used to consume MapR-DB CDC events
        KafkaConsumer<byte[], ChangeDataRecord> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList(CHANGE_LOG));

        while (true) {
            ConsumerRecords<byte[], ChangeDataRecord> changeRecords = consumer.poll(500);
            Iterator<ConsumerRecord<byte[], ChangeDataRecord>> iter = changeRecords.iterator();

            while (iter.hasNext()) {
                ConsumerRecord<byte[], ChangeDataRecord> crec = iter.next();
                // The ChangeDataRecord contains all the changes made to a document
                ChangeDataRecord changeDataRecord = crec.value();
                Iterator<KeyValue<FieldPath, ChangeNode>> cdcNodes = changeDataRecord.iterator();
                while (cdcNodes.hasNext()) {
                    Map.Entry<FieldPath, ChangeNode> changeNodeEntry = cdcNodes.next();
                    String fieldPathAsString = changeNodeEntry.getKey().asPathString();
                    ChangeNode changeNode = changeNodeEntry.getValue();
                    String documentId = changeDataRecord.getId().getString();

                    System.out.print(changeDataRecord.getType().name() + ": _id='" + documentId + "'");
                    if (changeDataRecord.getType() == ChangeDataRecordType.RECORD_UPDATE) {
                        System.out.print("  FieldPath: '" + fieldPathAsString + "'");
                        System.out.print("  ChangeOp: " + changeNode.getOp().name());
                        System.out.print("  FieldType: " + changeNode.getType().name());
                        System.out.println("  Value: " + changeNode.getValue());
                    } else {
                    }
                    System.out.println();
                }
                System.out.println("---");
            }
        }

    }

}
