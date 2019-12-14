#!/usr/bin/env bash

PROJECT_NAME=lekafgo-hackday

cd local_build/
java -cp "./${PROJECT_NAME}/${PROJECT_NAME}-libs/*" -Dlog4j.configuration=file:./${PROJECT_NAME}/config/log4j.properties \
            kafka.tools.StreamsResetter  --input-topics test --application-id=single-thread-consumer-app \
                  --bootstrap-servers localhost:9092

java -cp "./${PROJECT_NAME}/${PROJECT_NAME}-libs/*" -Dlog4j.configuration=file:./${PROJECT_NAME}/config/log4j.properties \
            kafka.tools.StreamsResetter  --input-topics test --application-id=multi-thread-consumer-app \
                  --bootstrap-servers localhost:9092

java -cp "./${PROJECT_NAME}/${PROJECT_NAME}-libs/*" -Dlog4j.configuration=file:./${PROJECT_NAME}/config/log4j.properties \
                 demo.driver.TopicLoader --topic test \
                 --num-records 100000 --payload-file ./${PROJECT_NAME}/config/payload.txt \
                 --producer.config ./${PROJECT_NAME}/config/producer.properties
