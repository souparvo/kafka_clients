from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import socket
from datetime import datetime
import sys
import logging
import yaml

## LOGGING
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

## Open config file
try:
    with open(sys.argv[1]) as fp:
        config = yaml.load(fp)
except OSError as e:
    logging.error("Error opening file: %s" % e)
    exit(1)

# Config vars
KAFKA_BROKERS = config['kafka_brokers']
THREAD = config['kafka_topic_part']
TOPIC = config['kafka_topic']

def create_topic(topic, part=3, repl=2):
    conf = {
        "bootstrap.servers": KAFKA_BROKERS
    }
    admin_client = AdminClient(conf)
    admin_client.create_topics([NewTopic(topic, part, repl)])

    print("TOPIC - [%s] CREATED" % topic)

def check_topic(topic):
    conf = {
        "bootstrap.servers": KAFKA_BROKERS
        # "zookeeper.connect": ZOOKEEPERS
    }

    admin_client = AdminClient(conf)

    if topic in admin_client.list_topics().topics:
        return True
    else:
        return False

def procude_million(topic):

    interval = 10000
    start_ts = datetime.utcnow()
    conf = {
        'bootstrap.servers': KAFKA_BROKERS,
        'client.id': topic
    }

    producer = Producer(conf)
    # Create message
    value = '_'.join([topic, start_ts.strftime("%Y%m%d%H%M%S"), str(THREAD)])
    logging.info("Message: %s" % value)
    count = 0
    total_prods = 0
    while count < 1000000:
        i = 0
        while i < interval:
            producer.produce(topic, value=value, partition=THREAD) #, callback=acked)

            # Wait up to 1 second for events. Callbacks will be invoked during
            # this method call if the message is acknowledged.
            prods = producer.poll(1.0)
            total_prods = total_prods + prods
            i = i + 1
     
        logging.info("Produced messages: %d - Avg Rate: %d events/s" % (total_prods, int(total_prods / (datetime.utcnow() - start_ts).total_seconds())))
        count = count + i
        

    return total_prods

if __name__ == '__main__':

    start = datetime.utcnow()
    logging.info("Starting...")
    procude_million(TOPIC)
    logging.info("Time elapsed: %s" % str(start - datetime.utcnow()))
    logging.info("End")
