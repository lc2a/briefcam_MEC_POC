from kafka import KafkaProducer
import os
import logging
from datetime import datetime

hostname = os.getenv("hostname", default=None)
cont_id = os.popen("cat /proc/self/cgroup | grep \"cpu:/\" | sed \'s/\([0-9]\):cpu:\/docker\///g\'").read()


def logging_to_console_and_syslog(log):
    logging.debug(log)
    i = datetime.now()
    print(str(i) + " hostname={} containerID={} ".format(hostname, cont_id[:5]) + log)


broker_name = os.getenv("broker_name_key", default=None)
logging_to_console_and_syslog("broker_name={}".format(broker_name))
topic = os.getenv("topic_key", default=None)

logging_to_console_and_syslog("topic={}".format(topic))

def post_filename_to_a_kafka_topic(filename):
    producer = KafkaProducer(bootstrap_servers=broker_name)


    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    logging_to_console_and_syslog(
        "Posting filename={} into kafka broker={}, topic={}".format(filename, broker_name, topic))
    producer.send(topic, filename.encode('utf-8'))

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()
