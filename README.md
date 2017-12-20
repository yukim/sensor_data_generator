# Sensor data generator for spark book

This program generates fake sensor data in JSON format, and publishes to Apache Kafka topic.

## Dependencies

- confluent_kafka
- faker
- simplejson

To install confluent_kafka, you need to install `librdkafka-devel` in CentOS.

```
sudo yum install librdkafka-devel
```

Then, install dependencies as follows:

```
pip install -r requirements.txt
```

## Usage

```
$ python main.py -h
usage: main.py [-h] [--kafka-brokers [KAFKA_BROKERS]]
               [--kafka-topic [KAFKA_TOPIC]] [--rate [RATE]]
               [--start-date [START_DATE]] [--num-sensors [NUM_SENSORS]]

Sensor data generator for spark book

optional arguments:
  -h, --help            show this help message and exit
  --kafka-brokers [KAFKA_BROKERS], -k [KAFKA_BROKERS]
                        Comma-separated list of kafka brokers. If specified,
                        data is published to kafka.
  --kafka-topic [KAFKA_TOPIC], -t [KAFKA_TOPIC]
                        Topic name to publish sensor data(default:
                        sensor_topic).
  --rate [RATE], -r [RATE]
                        Number of messages per second to generate(default:
                        10).
  --start-date [START_DATE], -s [START_DATE]
                        Start date (in YYYY-mm-dd format) of generated sensor
                        data
  --num-sensors [NUM_SENSORS], -n [NUM_SENSORS]
                        Number of sensors(default: 10)
```

The following command generates fake sensor data from one sensor in 5 min interval, and publishes to kafka broker at `192.168.33.10` topic `sensor_topic`. To stop generating message, simple terminate the program(Ctrl-C).

```
python main.py -k 192.168.33.10:9092 -t sensor_topic -r 100 -n 1
```


