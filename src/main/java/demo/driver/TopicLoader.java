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

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

public class TopicLoader {

  public static void main(String[] args) throws Exception {
    final ArgumentParser parser = argParser();

    final Random random = new Random(0);

    try {
      Namespace res = parser.parseArgs(args);

      /* parse args */
      String topicName = res.getString("topic");
      long numRecords = res.getLong("numRecords");
      Integer recordSize = res.getInt("recordSize");
      List<String> producerProps = res.getList("producerConfig");
      String producerConfig = res.getString("producerConfigFile");
      String payloadFilePath = res.getString("payloadFile");

      // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
      String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");

      if (producerProps == null && producerConfig == null) {
        throw new ArgumentParserException("Either --producer-props or --producer.config must be specified.", parser);
      }

      List<byte[]> payloadByteList = new ArrayList<>();
      if (payloadFilePath != null) {
        Path path = Paths.get(payloadFilePath);
        System.out.println("Reading payloads from: " + path.toAbsolutePath());
        if (Files.notExists(path) || Files.size(path) == 0)  {
          throw new  IllegalArgumentException("File does not exist or empty file provided.");
        }

        String[] payloadList = new String(Files.readAllBytes(path), "UTF-8").split(payloadDelimiter);

        System.out.println("Number of messages read: " + payloadList.length);

        for (String payload : payloadList) {
          payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
        }
      }

      Properties props = new Properties();
      if (producerConfig != null) {
        props.putAll(Utils.loadProps(producerConfig));
      }
      if (producerProps != null)
        for (String prop : producerProps) {
          String[] pieces = prop.split("=");
          if (pieces.length != 2)
            throw new IllegalArgumentException("Invalid property: " + prop);
          props.put(pieces[0], pieces[1]);
        }

      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

      KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);

      for (long i = 0; i < numRecords; i++) {
        if (i % 10000 == 0) {
          System.out.print(".");
        }

        byte[] payload = null;
        if (recordSize != null) {
          payload = new byte[recordSize];
          for (int j = 0; j < payload.length; ++j)
            payload[j] = (byte) (random.nextInt(26) + 65);
        }
        ProducerRecord<byte[], byte[]> record;

        if (payloadFilePath != null) {
          payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
        }
        record = new ProducerRecord<>(topicName, payload);

        producer.send(record);
      }

      producer.flush();
      producer.close();
    } catch (ArgumentParserException e) {
      if (args.length == 0) {
        parser.printHelp();
        Exit.exit(0);
      } else {
        parser.handleError(e);
        Exit.exit(1);
      }
    }

    System.out.println("\nDONE");
  }

  /** Get the command-line argument parser. */
  private static ArgumentParser argParser() {
    ArgumentParser parser = ArgumentParsers
        .newArgumentParser("topic-loader")
        .defaultHelp(true)
        .description("This tool is used to verify the producer performance.");

    MutuallyExclusiveGroup payloadOptions = parser
        .addMutuallyExclusiveGroup()
        .required(true)
        .description("either --record-size or --payload-file must be specified but not both.");

    parser.addArgument("--topic")
        .action(store())
        .required(true)
        .type(String.class)
        .metavar("TOPIC")
        .help("produce messages to this topic");

    parser.addArgument("--num-records")
        .action(store())
        .required(true)
        .type(Long.class)
        .metavar("NUM-RECORDS")
        .dest("numRecords")
        .help("number of messages to produce");

    payloadOptions.addArgument("--record-size")
        .action(store())
        .required(false)
        .type(Integer.class)
        .metavar("RECORD-SIZE")
        .dest("recordSize")
        .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.");

    payloadOptions.addArgument("--payload-file")
        .action(store())
        .required(false)
        .type(String.class)
        .metavar("PAYLOAD-FILE")
        .dest("payloadFile")
        .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
            "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
            "Note that you must provide exactly one of --record-size or --payload-file.");

    parser.addArgument("--payload-delimiter")
        .action(store())
        .required(false)
        .type(String.class)
        .metavar("PAYLOAD-DELIMITER")
        .dest("payloadDelimiter")
        .setDefault("\\n")
        .help("provides delimiter to be used when --payload-file is provided. " +
            "Defaults to new line. " +
            "Note that this parameter will be ignored if --payload-file is not provided.");

    parser.addArgument("--producer-props")
        .nargs("+")
        .required(false)
        .metavar("PROP-NAME=PROP-VALUE")
        .type(String.class)
        .dest("producerConfig")
        .help("kafka producer related configuration properties like bootstrap.servers,client.id etc. " +
            "These configs take precedence over those passed via --producer.config.");

    parser.addArgument("--producer.config")
        .action(store())
        .required(false)
        .type(String.class)
        .metavar("CONFIG-FILE")
        .dest("producerConfigFile")
        .help("producer config properties file.");

    return parser;
  }
}
