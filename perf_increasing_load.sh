# 1. clean up topics
# 2. pass in perf parameter
# 3. increase producer number
#############################
KAFKA_BROKER_ADDR=localhost:9092
TOPIC_NAME="test-topic"
PARTITION=3
INTERVAL=10 # in seconds
THROUGHPUT_CAP=-1
NUM_RECORDS=100
BROKER_CAP=10

# Usage: ./perf_increasing_load.sh -t test-topic -i 10 -p 1000
helpFunction()
{
    echo ""
    echo "Usage: [options]"
    echo "Options:"
    echo "-t topic name"
    echo "-i producer start intervals"
    echo "-p throughput"
    exit 1
}

while getopts "t:i:p:" q; do
   case "${q}" in
      t ) TOPIC_NAME=${OPTARG} ;;
      i ) INTERVAL=${OPTARG} ;;
      p ) THROUGHPUT_CAP=${OPTARG} ;;
      ? ) helpFunction ;;
   esac
done

echo "====> Clean up old topic: $TOPIC_NAME"
./kafka/bin/kafka-topics.sh --delete --bootstrap-server $KAFKA_BROKER_ADDR --topic $TOPIC_NAME

echo "====> Verify Deletion:"
./kafka/bin/kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER_ADDR

for (( i=1; i<=$BROKER_CAP; i++ ))
do
  echo "===>Starting producer $i at $(date)"
  ./kafka/bin/kafka-producer-perf-test.sh --producer-props bootstrap.servers=localhost:9092 --topic $TOPIC_NAME --throughput $THROUGHPUT_CAP --record-size 1000 --num-records $NUM_RECORDS > /dev/null 2>&1 &
  sleep $INTERVAL
done
