from kafka import KafkaProducer
import os
import sys, traceback

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
import time
from parent_producer import ParentProducer


class Kafka_Producer(ParentProducer):
    """
    This class does the following:
    1. It instantiates KAFKA producer instance.
    2. It posts a message to a pre-defined topic.
    wurstmeister KAFKA APIs.
    """

    def __init__(self):
        self.producer_instance = None
        self.broker_name = None
        self.topic = None
        super().__init__()
        self.read_environment_variables()
        self.connect_to_kafka_broker()

    def read_environment_variables(self):
        """
        This method is used to read the environment variables defined in the OS.
        :return:
        """
        while self.broker_name is None or \
                self.topic is None:
            time.sleep(2)
            logging_to_console_and_syslog("Kafka_Producer: Trying to read the environment variables...")
            self.broker_name = os.getenv("broker_name_key", default=None)
            self.topic = os.getenv("topic_key", default=None)
        logging_to_console_and_syslog("Kafka_Producer broker_name={}".format(self.broker_name))
        logging_to_console_and_syslog("Kafka_Producer topic={}".format(self.topic))

    def connect_to_kafka_broker(self):
        """
        This method tries to connect to the kafka broker based upon the type of kafka.
        :return:
        """
        while self.producer_instance is None:
            try:
                self.producer_instance = KafkaProducer(bootstrap_servers=self.broker_name)
            except:
                print("Exception in user code:")
                print("-" * 60)
                traceback.print_exc(file=sys.stdout)
                print("-" * 60)
                time.sleep(5)
            else:
                logging_to_console_and_syslog("ParentProducer Successfully connected to broker_name={}"
                                              .format(self.broker_name))

    def post_message(self, filename):
        """
        This method tries to post a message to the pre-defined kafka topic.
        :param filename:
        :return:
        """
        if filename is None or len(filename) == 0:
            logging_to_console_and_syslog("filename is None or invalid")
            return
        if self.producer_instance is None:
            logging_to_console_and_syslog("ParentProducer instance is None")
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
            future = self.producer_instance.send(self.topic, value)
            result = future.get(timeout=60)
        except:
            print("Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
        else:
            event = "Posting filename={} into kafka broker={}, topic={}, result = {}".format(filename,
                                                                                             self.broker_name,
                                                                                             self.topic,
                                                                                             result)
            logging_to_console_and_syslog(event)
            super().write_an_event_in_redis_db(event)
            super().increment_job_produced_count()
            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            self.producer_instance.flush()

    def cleanup(self):
        self.producer_instance.close()
