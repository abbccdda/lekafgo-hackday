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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    final long numThreads = Long.parseLong(getRequiredProperty("num.threads", properties));

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
      enterMultiThreadMode(consumer, pollDuration, recordProcessingTime, numThreads);
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
                                           long numThreads) {
    List<RecordKeyRange> keyRanges = splitKeyRange(numThreads);

    ConcurrentHashMap<Long, List<OffsetData>> recordMap = new ConcurrentHashMap<>();

    ExecutorService executorService = Executors.newFixedThreadPool((int) numThreads);
    List<WorkerThread> workers = new ArrayList<>();
    for (RecordKeyRange range : keyRanges) {
      WorkerThread workerThread = new WorkerThread(consumer, range, recordProcessingTime, recordMap);
      executorService.submit(workerThread);
      workers.add(workerThread);
    }

    while (!Thread.currentThread().isInterrupted()) {
      boolean needFetchMoreData = false;
      for (WorkerThread worker : workers) {
        needFetchMoreData = worker.drained();
      }

      if (needFetchMoreData) {
        final ConsumerRecords<byte[], byte[]> consumerRecords =
            consumer.poll(Duration.ofMillis(pollDuration));
        consumerRecords.forEach(record -> {
          long hashKey = (long) Arrays.hashCode(record.key());
          List<OffsetData> current = recordMap.getOrDefault(record.key(), new ArrayList<>());
          current.add(new OffsetData(record.topic(), record.partition(), record.offset()));
          recordMap.put(hashKey, current);
        });
      }
    }
    executorService.shutdown();
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

  enum WorkerState {
    RUNNING,
    DRAINED
  }
  static class WorkerThread extends Thread {

    final Consumer<byte[], byte[]> consumer;
    final RecordKeyRange range;
    final long recordProcessingTime;
    final ConcurrentHashMap<Long, List<OffsetData>> recordMap;
    private volatile WorkerState state;


    WorkerThread(final Consumer<byte[], byte[]> consumer,
                 final RecordKeyRange range,
                 final long recordProcessingTime,
                 final ConcurrentHashMap<Long, List<OffsetData>> recordMap) {
      this.consumer = consumer;
      this.range = range;
      this.recordProcessingTime = recordProcessingTime;
      this.recordMap = recordMap;
      this.state = WorkerState.RUNNING;
    }

    public boolean drained() {
      return state == WorkerState.DRAINED;
    }

    @Override
    public void run() {
      System.out.println("Thread " + Thread.currentThread().getId() + " is up and running");

      while (!Thread.currentThread().isInterrupted()) {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
        boolean noProgress = true;
        for (long key = range.lowerBound(); key <= range.upperBound(); key++) {
          if (recordMap.containsKey(key)) {
            state = WorkerState.RUNNING;
            long numRecords = recordMap.get(key).size();
            recordMap.put(key, new ArrayList<>());
            try {
              Thread.sleep(recordProcessingTime * numRecords);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            noProgress = false;
          }
        }

        if (noProgress) {
          state = WorkerState.RUNNING;
        } else {
          commit(endOffsets);
        }
      }
    }

    synchronized void commit(Map<TopicPartition, Long> endOffsets) {
      Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = new HashMap<>();
      for (Map.Entry<TopicPartition, Long> endOffset : endOffsets.entrySet()) {
        offsetAndMetadata.put(endOffset.getKey(), new OffsetAndMetadata(endOffset.getValue(), range, null));
      }
      consumer.commitSync(offsetAndMetadata);
    }
  }

  private static List<RecordKeyRange> splitKeyRange(long numThreads) {
    long interval = Integer.MAX_VALUE/numThreads;
    List<RecordKeyRange> keyRanges = new ArrayList<>();
    long startPoint = 0;

    for (int i = 0; i < numThreads; i++) {
      keyRanges.add(new RecordKeyRange(startPoint, startPoint + interval - 1));
      startPoint += interval;
    }
    return keyRanges;
  }

  static class OffsetData {
    final String topic;
    final int partition;
    final long offset;

    OffsetData(final String topic,
               int partition,
               long offset) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
    }
  }
}
