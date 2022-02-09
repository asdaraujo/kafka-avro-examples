/*
 * Copyright 2022 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.examples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class TransactionConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionConsumer.class);

    private final String topicName;
    private final Properties props = new Properties();

    public TransactionConsumer(String topicName, String propsFile) throws IOException {
        try (FileInputStream fileInputStream = new FileInputStream(propsFile)) {
            props.load(fileInputStream);
        }
        this.topicName = topicName;
    }

    private void consumeData() {
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        while (true) {
            ConsumerRecords<String, Transaction> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Transaction> record : records) {
                Transaction transaction = record.value();
                LOG.info("Received message: (" + record.key() + ", " + transaction + ") at partition " + record.partition() + ", offset " + record.offset()
                        + " with headers : " + Arrays.toString(record.headers().toArray()));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Syntax: " + TransactionProducer.class.getName() + " <topic> <properties_file>");
            System.exit(1);
        }
        String topicName = args[0];
        String propsFile = args[1];
        TransactionConsumer consumer = new TransactionConsumer(topicName, propsFile);
        consumer.consumeData();
    }
}
