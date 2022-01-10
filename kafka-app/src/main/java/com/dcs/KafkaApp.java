package com.dcs;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaApp {
  private static final String DEMO_SOURCE_TOPIC = "topic1";
  private static final String DEMO_TARGET_TOPIC = "topic2";

  private static final int SOURCE_POLL_TIMEOUT = 10;          // in hours
  private static final int TARGET_STATUS_TIMEOUT = 30;          // in sec

  private static String consumerConfigFile;
  private static String producerConfigFile;
  private static Consumer<String, String> consumer = null;
  private static Producer<String, String> producer = null;

  public static void main(String... args) {
    if (args.length != 2) {
      System.out.println("Usage: java -jar kafka-app-VERSION.jar PRODUCER CONSUMER");
      System.out.println("CONSUMER is a Kafka consumer username.");
      System.out.println("PRODUCER is a Kafka producer username.");
      System.exit(0);
    }
    consumerConfigFile = "/data/properties/consumer_" + args[0] + ".properties";
    producerConfigFile = "/data/properties/producer_" + args[1] + ".properties";
    getConsumer();
    getProducer();
    while (true) {
      ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofHours(SOURCE_POLL_TIMEOUT));
      if (!consumerRecords.isEmpty()) {
        consumerRecords.forEach(r -> {
          String key = r.key();
          String val = r.value();
          System.out.printf("Got a record: (%s, %s, %d, %d)\n", key, val, r.partition(), r.offset());
          producer.beginTransaction();
          ProducerRecord<String, String> rec = new ProducerRecord<>(DEMO_TARGET_TOPIC, key, val);
          try {
            RecordMetadata status = producer.send(rec).get(TARGET_STATUS_TIMEOUT, TimeUnit.SECONDS);
            producer.commitTransaction();
            System.out.println("Successfully committed " + key + " in topic " + DEMO_TARGET_TOPIC +
                " (partition:" + status.partition() + ", offset:" + status.offset() + ")");
          } catch (ExecutionException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
          }
        });
        consumer.commitAsync();
      }
    }
  }

  private static void getConsumer() {
    if (consumer == null) {
      Properties props = new Properties();
      try (InputStream propsStream = new FileInputStream(consumerConfigFile)) {
        props.load(propsStream);
      } catch (Exception e) {
        System.out.println("Failed loading the consumer config file");
        System.exit(1);
      }
      consumer = new KafkaConsumer<>(props);
      consumer.subscribe(Collections.singletonList(DEMO_SOURCE_TOPIC));
    }
  }

  private static void getProducer() {
    if (producer == null) {
      Properties props = new Properties();
      try (InputStream propsStream = new FileInputStream(producerConfigFile)) {
        props.load(propsStream);
      } catch (Exception e) {
        System.out.println("Failed loading the producer config file");
        System.exit(1);
      }
      producer = new KafkaProducer<>(props);
      producer.initTransactions();
    }
  }
}
