# 1. clean up topics
# 2. pass in perf parameter
# 3. increase producer number
#############################
KAFKA_BROKER_ADDR=localhost:9092
TOPIC_NAME="test-topic"
PARTITION=3
INTERVAL=10 # in seconds
THROUGHPUT_CAP=100 # bytes/second
NUM_RECORDS=1000
STEP=10

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

for (( i=1; i<=$STEP; i++ ))
do
  echo "===>Starting producer $i at $(date)"
  ./kafka/bin/kafka-producer-perf-test.sh --producer-props bootstrap.servers=localhost:9092 --topic $TOPIC_NAME --throughput $THROUGHPUT_CAP --record-size 1000 --num-records $NUM_RECORDS >> perf.log
  THROUGHPUT_CAP=$(($THROUGHPUT_CAP + 100))
  echo "Increase throughput to $THROUGHPUT_CAP"
  sleep $INTERVAL
done
