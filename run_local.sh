#!/usr/bin/env bash

PROJECT_NAME=lekafgo-hackday
METRIC_FILE_DIR=/tmp/consumer-driver/

./gradlew distTar
cp build/distributions/${PROJECT_NAME}.tar.gz local_build/
cd local_build
tar zxvf ${PROJECT_NAME}.tar.gz
cd ${PROJECT_NAME}
tar zxvf ${PROJECT_NAME}-libs.tar.gz
cd ../

mkdir -p ${METRIC_FILE_DIR}
touch ${METRIC_FILE_DIR}/$1-thread-consumer.csv
java -cp "./${PROJECT_NAME}/${PROJECT_NAME}-libs/*" -Dlog4j.configuration=file:./${PROJECT_NAME}/config/log4j.properties \
                 demo.driver.ConsumerAppDriver ./${PROJECT_NAME}/config/$1-thread-consumer.properties
