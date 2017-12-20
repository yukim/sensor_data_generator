import argparse, decimal
import simplejson as json

from datetime import datetime, timedelta
from confluent_kafka import Producer
from faker import Faker
from time import sleep, time

F = Faker()

class Sensor(object):
    def __init__(self, id, start_date):
        self.id = id
        self.coord = {"lon": round(F.longitude(), 2),
                      "lat": round(F.latitude(), 2)}
        self.temperature = float(decimal.Decimal(F.random.randrange(100, 300))/10)
        self.humidity = F.random_int(min=90, max=99)
        self.ph = float(decimal.Decimal(F.random.randrange(70, 100))/10)
        self.whc = self.humidity - 30 - F.random_int(min=1, max=10) + float(F.random.randrange(1, 9)/10)
        self.current_dt = datetime.combine(start_date, datetime.min.time())

    def __iter__(self):
        return self

    def next(self):
        message = json.dumps({"id": self.id,
                              "coord": self.coord,
                              "date": self.current_dt.strftime("%Y-%m-%d %H:%M"),
                              "main": {"temperature": self.temperature,
                                       "humidity": self.humidity,
                                       "ph": self.ph,
                                       "whc": self.whc}})
        # update data
	self.current_dt = self.current_dt + timedelta(minutes=5)
        self.temperature = round(self.temperature + float(decimal.Decimal(F.random.randrange(-10, 10))/100), 1)
        self.humidity = min(99, self.humidity + F.random_int(min=-1, max=1))
        self.ph = round(max(self.ph + float(decimal.Decimal(F.random.randrange(-30, 30))/100), 5.0), 1)
        self.whc = self.humidity - 30 - F.random_int(min=1, max=10) + float(F.random.randrange(1, 9)/10)
        return message

class SensorDataGenerator(object):
    def __init__(self,
                 start_id,
                 start_date,
                 num_sensors=10):
        self.current_dt = datetime.combine(start_date, datetime.min.time())
        self.sensors = [Sensor(i, self.current_dt) for i in range(start_id, start_id + num_sensors)]
        self.current_pos = -1

    def __iter__(self):
        return self

    def next(self):
        self.current_pos += 1
        return self.sensors[self.current_pos % len(self.sensors)].next()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Sensor data generator for spark book")
    parser.add_argument("--kafka-brokers", "-k",
                        nargs="?",
                        help="Comma-separated list of kafka brokers. If specified, data is published to kafka.")
    parser.add_argument("--kafka-topic", "-t",
                        nargs="?",
                        help="Topic name to publish sensor data(default: sensor_topic).")
    parser.add_argument("--rate", "-r",
                        nargs="?",
                        help="Number of messages per second to generate(default: 10).")
    parser.add_argument("--start-date", "-s",
                        nargs="?",
                        help="Start date (in YYYY-mm-dd format) of generated sensor data")
    parser.add_argument("--num-sensors", "-n",
                        nargs="?",
                        help="Number of sensors(default: 10)")

    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else F.past_date(start_date="-2y")
    sensors = int(args.num_sensors) if args.num_sensors else 10
    rate = float(args.rate) if args.rate else float(10)
    p = Producer({'bootstrap.servers': args.kafka_brokers}) if args.kafka_brokers else None
    topic = args.kafka_topic or "sensor_topic"
    generator = SensorDataGenerator(F.random_int(min=1000), start_date, num_sensors=sensors)

    interval = 1.0 / rate
    last = 0.0
    try:
        for data in generator:
            elapsed = time() - last
            wait = interval - elapsed
            if wait > 0:
                sleep(wait)
            print data
            if p is not None:
                p.produce(topic, data.encode('utf-8'))
            last = time()
    except KeyboardInterrupt:
        pass
    finally:
        if p is not None:
            p.flush()
