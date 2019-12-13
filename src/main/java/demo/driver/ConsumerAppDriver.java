/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package demo.driver;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RecordKeyRange;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;


public class ConsumerAppDriver {

  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.printf("Usage: %s CONFIG_FILE\n", ConsumerAppDriver.class.getName());
      System.err.println();
      System.err.println("    CONFIG_FILE - The configuration file to use for this test");
      System.exit(1);
    }

    Properties properties = new Properties();

    try {
      properties.load(new FileInputStream(args[0]));
    } catch (IOException e) {
      System.err.printf("Failed to load configuration file: %s.", args[0]);
      System.err.println(e.toString());
      System.exit(1);
    }

    final String inputTopicName = getRequiredProperty("test.topic.input.name", properties);
    final String threadingModel = getRequiredProperty("threading.model", properties);
    final long pollDuration = Long.parseLong(getRequiredProperty("consumer.poll.timeout.ms", properties));
    final long recordProcessingTime = Long.parseLong(getRequiredProperty("record.processing.time.ms", properties));

    properties = filterOutTestProperties(properties);

    properties.put(ConsumerConfig.GROUP_ID_CONFIG, threadingModel + "-consumer-app");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    final String inputTopic = properties.getProperty("input.topic", inputTopicName);

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(inputTopic));

    if (threadingModel.equals("single-thread")) {
      System.out.println("Entering single thread mode");
      enterSingleThreadMode(consumer, pollDuration, recordProcessingTime);
    } else {
      System.out.println("Entering multi thread mode");
      enterMultiThreadMode(consumer, pollDuration, recordProcessingTime, 3);
    }
  }

  private static void enterSingleThreadMode(Consumer<byte[], byte[]> consumer,
                                            long pollDuration,
                                            long recordProcessingTime) {

    while (true) {
      final ConsumerRecords<byte[], byte[]> consumerRecords =
          consumer.poll(Duration.ofMillis(pollDuration));

      consumerRecords.forEach(record -> {
        try {
          Thread.sleep(recordProcessingTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });
      consumer.commitSync();
    }
  }

  private static void enterMultiThreadMode(Consumer<byte[], byte[]> consumer,
                                           long pollDuration,
                                           long recordProcessingTime,
                                           int numThreads) {
    Map<TopicPartition, OffsetAndMetadata> currentUnsafeOffset = new HashMap<>();
    List<RecordKeyRange> keyRanges = splitKeyRange(numThreads);

    List<WorkerThread> workers = new ArrayList<>();
    for (RecordKeyRange range : keyRanges) {
      workers.add(new WorkerThread(consumer, range));
    }
    ConcurrentHashMap<byte[], List<Long>> recordMap = new ConcurrentHashMap<>();

    // Per key level max offset processed yet.
    ConcurrentHashMap<byte[], Long> processedOffset = new ConcurrentHashMap<>();

    while (true) {
      final ConsumerRecords<byte[], byte[]> consumerRecords =
          consumer.poll(Duration.ofMillis(pollDuration));

      consumerRecords.forEach(record -> {
        List<Long> current = recordMap.getOrDefault(record.key(), new ArrayList<>());
        current.add(record.offset());
        recordMap.put(record.key(), current);
      });
    }
  }

  private static String getRequiredProperty(final String propName, final Properties props) {
    String value = props.getProperty(propName);
    if (value != null) {
      return value;
    }

    System.err.printf("Required property '%s' not set in configuration\n", propName);
    System.err.flush();
    System.exit(1);
    return null;
  }

  private static Properties filterOutTestProperties(Properties properties) {
    final Properties filteredProps = new Properties();
    for (final String propertyName : properties.stringPropertyNames()) {
      if (!propertyName.startsWith("test.")) {
        filteredProps.setProperty(propertyName, properties.getProperty(propertyName));
      }
    }
    return filteredProps;
  }

  static class WorkerThread extends Thread {

    final Consumer<byte[], byte[]> consumer;
    final RecordKeyRange range;
    ConcurrentHashMap<byte[], List<Long>> recordMap;

    public WorkerThread(Consumer<byte[], byte[]> consumer, RecordKeyRange range) {
      this.consumer = consumer;
      this.range = range;
    }

    @Override
    public void run() {
      try {
        System.out.println("Thread " + Thread.currentThread().getId() + " is up and running");
        while (true) {

        }

      } catch (Exception e) {
        // Throwing an exception
        System.out.println("Exception is caught");
      }
    }

    public synchronized void commit() {
      consumer.commitSync();
    }
  }

  private static List<RecordKeyRange> splitKeyRange(int numThreads) {
    long interval = Long.MAX_VALUE/numThreads;
    List<RecordKeyRange> keyRanges = new ArrayList<>();
    long startPoint = 0;

    for (int i = 0; i < numThreads; i++) {
      keyRanges.add(new RecordKeyRange(startPoint, startPoint + interval - 1));
      startPoint += interval;
    }
    return keyRanges;
  }
}
