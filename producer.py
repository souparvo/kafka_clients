from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
import socket
import sys, os
import logging
import yaml

## LOGGING
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

DATA_DIR = '/'.join(os.path.realpath(__file__).split('/')[:-1]) + '/data'

## Functions
def open_yaml(path):
    """Open YAML file and produces a dict with contents. Requires 
    pyYaml package.

    Arguments:
        path {str} -- full path for the yaml/yml file

    Returns:
        dict -- content of the file
    """
    try:
        with open(path) as fp:
            contents = yaml.load(fp)
        return contents
    except OSError as e:
        logging.error("Error opening file: %s" % e)
        exit(1)

def write_to_file(path, value, mode='a'):
    """Writes line to file, appends by default

    Arguments:
        path {str} -- full path of the file
        value {str} -- valu to writo on the line

    Keyword Arguments:
        mode {str} -- write mode (default: {'a'})
    """
    try:
        with open(path, mode) as fp:
            fp.write(value + '\n')
    except OSError as e:
        logging.error("Error opening file: %s" % e)
        exit(1)

def create_topic(conf, topic, part=3, repl=2):
    """Creates topic on Kafka cluster @TODO

    Arguments:
        conf {[type]} -- [description]
        topic {[type]} -- [description]

    Keyword Arguments:
        part {int} -- [description] (default: {3})
        repl {int} -- [description] (default: {2})
    """

    admin_client = AdminClient(conf)
    admin_client.create_topics([NewTopic(topic, part, repl)])

    print("TOPIC - [%s] CREATED" % topic)

def check_topic(conf, topic):
    """Checks if topic exists on Kafka cluster

    Arguments:
        conf {dict} -- configuration dict as per Kafka docs for admin tools
        topic {str} -- topic name to check

    Returns:
        bool -- True if topic exist, False otherwise
    """

    admin_client = AdminClient(conf)

    if topic in admin_client.list_topics().topics:
        return True
    else:
        return False

def produce_messages(prod, topic, value, part):
    prod.produce(topic, value=value, partition=part)
    return prod.poll(1.0)

def procude_million(prod_config, topic, part, m_count=100, data_dir=DATA_DIR):
    """Produces messages to topic partition

    Arguments:
        prod_config {dict} -- producer configs as per Kafka docs
        topic {str} -- topic to produce messages
        part {int} -- partition of the topic to write messages

    Keyword Arguments:
        m_count {int} -- # of messages to produce (default: {100})
        data_dir {str} -- output file dir, defaults to data/ (default: {DATA_DIR})

    Returns:
        int -- number of messages produced
    """

    interval = 10000

    if m_count < 10000:
        interval = m_count
        
    conf = prod_config
    conf['client.id'] = topic

    producer = Producer(conf)
    # Create message
    start_ts = datetime.utcnow()
    value = '_'.join([topic, start_ts.strftime("%Y%m%d%H%M%S"), str(part)])
    logging.info("Message: %s" % value)
    count = 0
    total_prods = 0
    while count < m_count:
        i = 0
        while i < interval:
            producer.produce(topic, value=value, partition=part)
            prods = producer.poll(1.0)
            total_prods = total_prods + prods
            i = i + 1
    
        logging.info("Produced messages: %d - Avg Rate: %d events/s" % (total_prods, int(total_prods / (datetime.utcnow() - start_ts).total_seconds())))
        count = count + i
    # Write to csv file
    file_path = data_dir + '/' + '_'.join([topic,str(part)]) + '.csv'
    write_to_file(file_path, ','.join([value, str(total_prods)]))

    return total_prods

## MAIN
if __name__ == '__main__':
    # Receive script argument with config file
    config = open_yaml(sys.argv[1])
    # Register scrip start
    start = datetime.utcnow()
    logging.info("Starting...")
    # Produce X messages to topic partition
    procude_million(config['kafka_producer_config'], config['kafka_topic'], config['kafka_topic_part'], config['message_count'])
    # Register time elapsed
    logging.info("Time elapsed: %s" % str(datetime.utcnow() - start))
    logging.info("End")
