from kafka import KafkaProducer
import os
import sys, traceback
sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog

class Producer:
    def __init__(self):
        self.producer_instance=None
        self.broker_name = None
        self.topic = None
        self.create_producer_instance()

    def create_producer_instance(self):
        while self.broker_name is None and self.topic is None:
            self.broker_name = os.getenv("broker_name_key", default=None)
            self.topic = os.getenv("topic_key", default=None)
        self.producer_instance = KafkaProducer(bootstrap_servers=self.broker_name)
        logging_to_console_and_syslog("broker_name={}".format(self.broker_name))
        logging_to_console_and_syslog("topic={}".format(self.topic))

    def post_filename_to_a_kafka_topic(self,filename):
        if filename is None or len(filename) == 0:
            logging_to_console_and_syslog("filename is None or invalid")
            return
        if self.producer_instance is None:
            logging_to_console_and_syslog("Producer instance is None")
            return

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        logging_to_console_and_syslog(
            "Posting filename={} into kafka broker={}, topic={}".format(filename,
                                                                        self.broker_name,
                                                                        self.topic))
        value = filename.encode('utf-8')
        try:
            self.producer_instance.send(self.topic, value)
            result = self.producer_instance.get(timeout=60)
        except:
            print("Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
        else:
            logging_to_console_and_syslog(
            "Posting filename={} into kafka broker={}, topic={}, result = {}".format(filename,
                                                                        self.broker_name,
                                                                        self.topic,
                                                                        result))

            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            self.producer_instance.flush()

    def close_producer_instance(self):
        self.producer_instance.close()
