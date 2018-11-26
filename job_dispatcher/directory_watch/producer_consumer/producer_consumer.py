from kafka import KafkaProducer
import os
import sys, traceback

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
import time
from kafka_msgq_api.kafka_msgq_api import KafkaMsgQAPI


class ProducerConsumerAPI:
    """
    This is a factory design pattern.
    This class produces messages into
    1. Kafka Queue.
    2. Rabbit Message Queue.
    """
    rabbitMsgQType = "Rabbit"
    kafkaMsgQType = "Kafka"

    def __init__(self):
        self.producer_instance = None
        self.consumer_instance = None
        self.type_of_messaging_queue = None
        self.read_environment_variables()

    def read_environment_variables(self):
        """
        This method is used to read the environment variables defined in the OS.
        :return:
        """
        while self.type_of_messaging_queue is None:
            time.sleep(2)
            logging_to_console_and_syslog("ProducerConsumerAPI: "
                                          "Trying to read the environment variables...")
            self.type_of_messaging_queue = os.getenv("type_of_messaging_queue_key",
                                                     default=None)
        logging_to_console_and_syslog("ProducerConsumerAPI:"
                                      "type_of_messaging_queue={}"
                                      .format(self.type_of_messaging_queue))

    def connect(self,
                is_producer=False,
                is_consumer=False):
        """
        This method tries to connect to the messaging queue.
        :return:
        """
        while self.producer_instance is None:
            try:
                if self.type_of_messaging_queue == ProducerConsumerAPI.kafkaMsgQType:
                    self.producer_instance = KafkaMsgQAPI(is_producer, is_consumer)
                elif self.type_of_messaging_queue == ProducerConsumerAPI.rabbitMsgQType:
                    self.producer_instance = RabbitMsgQAPI(is_producer, is_consumer)
            except:
                print("Exception in user code:")
                print("-" * 60)
                traceback.print_exc(file=sys.stdout)
                print("-" * 60)
                time.sleep(5)
            else:
                logging_to_console_and_syslog("ProducerConsumerAPI: Successfully "
                                              "created producer instance for messageQ type ={}"
                                              .format(self.type_of_messaging_queue))

    def enqueue(self, filename):
        """
        This method tries to post a message.
        :param filename:
        :return:
        """
        if filename is None or len(filename) == 0:
            logging_to_console_and_syslog("filename is None or invalid")
            return

        if self.producer_instance is None:
            self.connect(is_producer=True)

        if hasattr(self.type_of_messaging_queue, enqueue):
            self.type_of_messaging_queue.enqueue()

    def dequeue(self):
        if self.consumer_instance is None:
            self.connect(is_consumer=True)

        if hasattr(self.type_of_messaging_queue, dequeue):
            self.type_of_messaging_queue.dequeue()

    def cleanup(self):
        self.producer_instance.cleanup()
