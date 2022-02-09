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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;

public class TransactionProducer {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionProducer.class);
    private static final String[] CURRENCIES = new String[] {
            "AFA", "ALL", "DZD", "AOR", "ARS", "AMD", "AWG", "AUD", "AZN", "BSD", "BHD", "BDT", "BBD", "BYN", "BZD",
            "BMD", "BTN", "BOB", "BWP", "BRL", "GBP", "BND", "BGN", "BIF", "KHR", "CAD", "CVE", "KYD", "XOF", "XAF",
            "XPF", "CLP", "CNY", "COP", "KMF", "CDF", "CRC", "HRK", "CUP", "CZK", "DKK", "DJF", "DOP", "XCD", "EGP",
            "SVC", "ERN", "EEK", "ETB", "EUR", "FKP", "FJD", "GMD", "GEL", "GHS", "GIP", "XAU", "XFO", "GTQ", "GNF",
            "GYD", "HTG", "HNL", "HKD", "HUF", "ISK", "XDR", "INR", "IDR", "IRR", "IQD", "ILS", "JMD", "JPY", "JOD",
            "KZT", "KES", "KWD", "KGS", "LAK", "LVL", "LBP", "LSL", "LRD", "LYD", "LTL", "MOP", "MKD", "MGA", "MWK",
            "MYR", "MVR", "MRO", "MUR", "MXN", "MDL", "MNT", "MAD", "MZN", "MMK", "NAD", "NPR", "ANG", "NZD", "NIO",
            "NGN", "KPW", "NOK", "OMR", "PKR", "XPD", "PAB", "PGK", "PYG", "PEN", "PHP", "XPT", "PLN", "QAR", "RON",
            "RUB", "RWF", "SHP", "WST", "STD", "SAR", "RSD", "SCR", "SLL", "XAG", "SGD", "SBD", "SOS", "ZAR", "KRW",
            "LKR", "SDG", "SRD", "SZL", "SEK", "CHF", "SYP", "TWD", "TJS", "TZS", "THB", "TOP", "TTD", "TND", "TRY",
            "TMT", "AED", "UGX", "XFU", "UAH", "UYU", "USD", "UZS", "VUV", "VEF", "VND", "YER", "ZMK", "ZWL" };

    private final String topicName;
    private final Properties props = new Properties();
    private final Random rng = new Random(System.currentTimeMillis());

    public TransactionProducer(String topicName, String propsFile) throws IOException {
        try (FileInputStream fileInputStream = new FileInputStream(propsFile)) {
            props.load(fileInputStream);
        }
        this.topicName = topicName;
    }

    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (metadata != null) {
                LOG.info("Message produced to topic [{}], partition {}, offset {}", metadata.topic(), metadata.partition(), metadata.offset(), e);
            } else {
                LOG.warn("Producer request returned null metadata.", e);
            }
        }
    }

    private void produceData() throws InterruptedException {
        final KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);
        try {
            while (true) {
                Transaction transaction = new Transaction();
                transaction.setId(System.currentTimeMillis());
                transaction.setAccountId(Math.abs(rng.nextLong()) % 100);
                transaction.setAmount((rng.nextInt() % 2000000) / 100.0);
                transaction.setCurrency(CURRENCIES[Math.abs(rng.nextInt()) % CURRENCIES.length]);

                ProducerRecord<String, Transaction> record = new ProducerRecord<>(topicName, transaction);
                producer.send(record, new ProducerCallback());
                Thread.sleep(100);
            }
        } finally {
            producer.flush();
            producer.close(Duration.ofSeconds(5));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Syntax: " + TransactionProducer.class.getName() + " <topic> <properties_file>");
            System.exit(1);
        }
        String topicName = args[0];
        String propsFile = args[1];
        TransactionProducer producer = new TransactionProducer(topicName, propsFile);
        producer.produceData();
    }
}
