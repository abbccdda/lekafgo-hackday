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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class StreamsDriver {
  public static void main(String[] args) throws InterruptedException {
    if (args.length == 0) {
      System.err.printf("Usage: %s CONFIG_FILE\n", StreamsDriver.class.getName());
      System.err.println();
      System.err.println("    CONFIG_FILE - The configuration file to use for this test");
      System.exit(1);
    }

    Properties streamsProps = new Properties();

    try {
      streamsProps.load(new FileInputStream(args[0]));
    } catch (IOException e) {
      System.err.printf("Failed to load configuration file: %s.", args[0]);
      System.err.println(e.toString());
      System.exit(1);
    }

    final String inputTopicName = getRequiredProperty("test.topic.input.name", streamsProps);
    final String outputTopicName = getRequiredProperty("test.topic.output.name", streamsProps);
    final int numRecords = Integer.parseInt(getRequiredProperty("test.topic.input.num-records", streamsProps));
    final int numWarmUpRecords = Integer.parseInt(getRequiredProperty("test.num-warmup-records", streamsProps));

    streamsProps = filterOutTestProperties(streamsProps);

    streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
    streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
    streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());

    final StreamsBuilder builder = new StreamsBuilder();
    final String inputTopic = streamsProps.getProperty("input.topic", inputTopicName);

    final KStream<byte[], byte[]> originalStream = builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()));

    final CountDownLatch doneLatch = new CountDownLatch(numRecords);
    final AtomicLong bytesWritten = new AtomicLong();

    final CountDownLatch runningLatch = new CountDownLatch(1);
    final AtomicInteger iterCount = new AtomicInteger();
    final KStream<byte[], byte[]> mappedStatelessStream = originalStream.map(KeyValue::pair)
        .peek((k, v) -> {
          final int count = iterCount.getAndIncrement();
          if (count % 10000 == 0) {
            System.out.print(".");
          }
          if (count == numWarmUpRecords) {
            runningLatch.countDown();
          }
          if (count >= numWarmUpRecords) {
            bytesWritten.getAndAdd((k != null ? k.length : 0) + (v != null ? v.length : 0));
            doneLatch.countDown();
          }
        });

    mappedStatelessStream.to(outputTopicName, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));

    final Topology topology = builder.build(streamsProps);

    final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps);

    kafkaStreams.setUncaughtExceptionHandler((t, e) -> System.err.printf("Unexpected exception in streams: %s\n", e));

    kafkaStreams.cleanUp();

    kafkaStreams.start();

    runningLatch.await();

    final long startNanos = System.nanoTime();
    doneLatch.await();
    kafkaStreams.close();
    final long endNanos = System.nanoTime();

    double elapsedSecs = (double)(endNanos - startNanos) / (double)(1000 * 1000 * 1000);
    System.out.println();
    System.out.printf("Number of records: %d. Total time: %f ms. Throughput (MB/s): %f\n",
        numRecords, elapsedSecs * 1000 * 1000, (double)bytesWritten.get() / (double)(1024 * 1024) / elapsedSecs);
//    System.out.printf("Number of records: %d. Total time: %f ms. Throughput (MB/s): %f\n",
//                      numRecords, elapsedSecs * 1000, (double) (numRecords * 100) / (1000 * elapsedSecs));
    System.out.flush();
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

}
