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
import java.io.FileOutputStream;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RecordKeyRange;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

public class ConsumerAppDriver {

  static Logger log = LoggerFactory.getLogger(ConsumerAppDriver.class);
  static FileOutputStream outputStream;

  public static void main(String[] args) throws IOException {
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
    final String metricsFilePath = getRequiredProperty("metrics.file.path", properties);

    final String threadingModel = getRequiredProperty("threading.model", properties);
    final long pollDuration = Long.parseLong(getRequiredProperty("consumer.poll.timeout.ms", properties));
    final long recordProcessingTime = Long.parseLong(getRequiredProperty("record.processing.time.ms", properties));
    final long numThreads = Long.parseLong(getRequiredProperty("num.threads", properties));

    properties = filterOutTestProperties(properties);

    properties.put(ConsumerConfig.GROUP_ID_CONFIG, threadingModel + "-consumer-app");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Int.MaxValue());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);

    final String inputTopic = properties.getProperty("input.topic", inputTopicName);

    outputStream = new FileOutputStream(metricsFilePath);

    log.info("Starting writer at {}", metricsFilePath);

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singletonList(inputTopic));
    if (threadingModel.equals("single-thread")) {
      log.info("Entering single thread mode");
      enterSingleThreadMode(consumer, pollDuration, recordProcessingTime, inputTopic);
    } else {
      log.info("Entering multi thread mode");
      enterMultiThreadMode(consumer, pollDuration, recordProcessingTime, numThreads);
    }
  }

  private static void enterSingleThreadMode(Consumer<byte[], byte[]> consumer,
                                            long pollDuration,
                                            long recordProcessingTime,
                                            String inputTopic) throws IOException {

    int processedRecords = 0;
    while (true) {
      final ConsumerRecords<byte[], byte[]> consumerRecords =
          consumer.poll(Duration.ofMillis(pollDuration));

      long startTime = System.currentTimeMillis();
      long lastOffset = 0;
      for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
        try {
          Thread.sleep(recordProcessingTime);
          lastOffset = Math.max(lastOffset, record.offset());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      long finishTime = System.currentTimeMillis();
      processedRecords += consumerRecords.count();
      outputStream.write(String.format("%d,%d\n", processedRecords, finishTime).getBytes());
      log.info("Consumer processed records used {} seconds", (finishTime - startTime)/1000);

      if (consumerRecords.count() > 0) {
        OffsetAndMetadata metadata = new OffsetAndMetadata(lastOffset);
        consumer.commitSync(Collections.singletonMap(new TopicPartition(inputTopic, 0), metadata));
      }
    }
  }

  private static void enterMultiThreadMode(Consumer<byte[], byte[]> consumer,
                                           long pollDuration,
                                           long recordProcessingTime,
                                           long numThreads) throws IOException {
    List<RecordKeyRange> keyRanges = splitKeyRange(numThreads);

    ConcurrentHashMap<Long, List<OffsetData>> recordMap = new ConcurrentHashMap<>();

    ExecutorService executorService = Executors.newFixedThreadPool((int) numThreads);
    List<WorkerThread> workers = new ArrayList<>();
    for (RecordKeyRange range : keyRanges) {
      WorkerThread workerThread = new WorkerThread(consumer, range, recordProcessingTime, recordMap);
      executorService.execute(workerThread);
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

        for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
          long hashKey = (long) Arrays.hashCode(record.key());
          List<OffsetData> current = recordMap.getOrDefault(hashKey, new ArrayList<>());
          current.add(new OffsetData(record.topic(), record.partition(), record.offset()));
          recordMap.put(hashKey, current);
        }

        int totalProcessedRecords = 0;
        for (WorkerThread workerThread : workers) {
          totalProcessedRecords += workerThread.processedRecords();
        }
        outputStream.write(String.format("%d,%d\n", totalProcessedRecords, System.currentTimeMillis()).getBytes());
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
    private int processedRecords;

    WorkerThread(final Consumer<byte[], byte[]> consumer,
                 final RecordKeyRange range,
                 final long recordProcessingTime,
                 final ConcurrentHashMap<Long, List<OffsetData>> recordMap) {
      this.consumer = consumer;
      this.range = range;
      this.recordProcessingTime = recordProcessingTime;
      this.recordMap = recordMap;
      this.state = WorkerState.DRAINED;
      this.processedRecords = 0;
    }

    boolean drained() {
      return state == WorkerState.DRAINED;
    }

    @Override
    public void run() {
      log.info("Thread {} is up and running", Thread.currentThread().getId());

      while (!Thread.currentThread().isInterrupted()) {
        Map<TopicPartition, Long> maxFetchedOffsets = new HashMap<>();
        boolean noProgress = true;
        long startTime = System.currentTimeMillis();

        for (long key = range.lowerBound(); key <= range.upperBound(); key++) {
          int numRecords = recordMap.containsKey(key) ? recordMap.get(key).size() : 0;
          if (numRecords > 0) {
            state = WorkerState.RUNNING;

            for (OffsetData lastOffsetData: recordMap.get(key)) {
              TopicPartition partitionKey = new TopicPartition(lastOffsetData.topic, lastOffsetData.partition);
              if (maxFetchedOffsets.getOrDefault(partitionKey, 0L) < lastOffsetData.offset) {
                maxFetchedOffsets.put(partitionKey, lastOffsetData.offset);
              }
            }

            // Clear the map entry indicating a finished work.
            recordMap.put(key, new ArrayList<>());
            try {
              Thread.sleep(recordProcessingTime * numRecords);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            processedRecords += numRecords;
            noProgress = false;
          }
        }

        log.info("Thread {} processing used {} seconds", Thread.currentThread().getId(), (System.currentTimeMillis() - startTime)/1000);

        if (noProgress) {
          state = WorkerState.DRAINED;
        } else {
          commit(maxFetchedOffsets);
        }
      }
    }

    synchronized void commit(Map<TopicPartition, Long> endOffsets) {
      Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = new HashMap<>();
      for (Map.Entry<TopicPartition, Long> endOffset : endOffsets.entrySet()) {
        offsetAndMetadata.put(endOffset.getKey(), new OffsetAndMetadata(endOffset.getValue(), null, null));
      }
      consumer.commitSync(offsetAndMetadata);
    }

    int processedRecords() {
      return processedRecords;
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
