#!/usr/bin/env bash

PROJECT_NAME=lekafgo-hackday
./gradlew distTar
cp build/distributions/${PROJECT_NAME}.tar.gz local_build/
cd local_build
tar zxvf ${PROJECT_NAME}.tar.gz
cd ${PROJECT_NAME}
tar zxvf ${PROJECT_NAME}-libs.tar.gz
cd ../
java -cp "./${PROJECT_NAME}/${PROJECT_NAME}-libs/*" -Dlog4j.configuration=file:./${PROJECT_NAME}/config/log4j.properties \
                 demo.driver.ConsumerAppDriver ./${PROJECT_NAME}/config/multi-thread-consumer.properties
