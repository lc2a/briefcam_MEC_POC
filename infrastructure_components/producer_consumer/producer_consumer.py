from kafka import KafkaProducer
import os
import sys, traceback
import time

#sys.path.append("..")  # Adds higher directory to python modules path.

def import_all_packages():
    realpath=os.path.realpath(__file__)
    #print("os.path.realpath({})={}".format(__file__,realpath))
    dirname=os.path.dirname(realpath)
    #print("os.path.dirname({})={}".format(realpath,dirname))
    dirname_list=dirname.split('/')
    #print(dirname_list)
    for index in range(len(dirname_list)):
        module_path='/'.join(dirname_list[:index])
        #print("module_path={}".format(module_path))
        try:
            sys.path.append(module_path)
        except:
            #print("Invalid module path {}".format(module_path))
            pass

import_all_packages()

from infrastructure_components.log.log_file import logging_to_console_and_syslog
from infrastructure_components.producer_consumer.wurstmeister_kafka_msgq_api.kafka_msgq_api import KafkaMsgQAPI
from infrastructure_components.producer_consumer.rabbit_msgq_api.rabbit_msgq_api import RabbitMsgQAPI
from infrastructure_components.producer_consumer.confluent_kafka_msgq_api.confluent_kafka_msgq_api import ConfluentKafkaMsgQAPI
from infrastructure_components.redis_client.redis_interface import RedisInterface


class ProducerConsumerAPI:
    """
    This is a factory design pattern.
    This class produces messages into
    1. Kafka Queue.
    2. Rabbit Message Queue.
    """
    rabbitMsgQType = "Rabbit"
    kafkaMsgQType = "Kafka"
    confluentKafkaMsgQType = "ConfluentKafka"

    def __init__(self,
                 is_producer=False,
                 is_consumer=False,
                 perform_subscription=False,
                 type_of_messaging_queue=None,
                 thread_identifier=None):
        self.message_queue_instance = None
        self.redis_instance = None
        self.is_producer = is_producer
        self.is_consumer = is_consumer
        self.perform_subscription = perform_subscription
        self.type_of_messaging_queue = type_of_messaging_queue
        self.thread_identifier = thread_identifier
        self.read_environment_variables()
        #self.__connect()

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

    def __connect(self):
        """
        This method tries to connect to the messaging queue.
        :return:
        """
        if self.message_queue_instance is None:
            try:
                if self.type_of_messaging_queue == ProducerConsumerAPI.kafkaMsgQType:
                    self.message_queue_instance = KafkaMsgQAPI(is_producer=self.is_producer,
                                                               is_consumer=self.is_consumer,
                                                               perform_subscription=self.perform_subscription,
                                                               thread_identifier=self.thread_identifier)
                elif self.type_of_messaging_queue == ProducerConsumerAPI.rabbitMsgQType:
                    self.message_queue_instance = RabbitMsgQAPI(is_producer=self.is_producer,
                                                                is_consumer=self.is_consumer,
                                                                perform_subscription=self.perform_subscription,
                                                                thread_identifier=self.thread_identifier)
                elif self.type_of_messaging_queue == ProducerConsumerAPI.confluentKafkaMsgQType:
                    self.message_queue_instance = ConfluentKafkaMsgQAPI(is_producer=self.is_producer,
                                                                        is_consumer=self.is_consumer,
                                                                        perform_subscription=self.perform_subscription,
                                                                        thread_identifier=self.thread_identifier)
                if not self.redis_instance:
                    if self.is_producer:
                        self.redis_instance = RedisInterface("Producer{}".format(self.thread_identifier))
                    elif self.is_consumer:
                        self.redis_instance = RedisInterface("Consumer{}".format(self.thread_identifier))
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
        :return True or False:
        """
        status = False

        if filename is None or len(filename) == 0:
            logging_to_console_and_syslog("filename is None or invalid")
            return status

        if self.message_queue_instance is None:
            self.__connect()

        if hasattr(self.message_queue_instance, 'enqueue'):
            status = self.message_queue_instance.enqueue(filename)
            event = "Producer: Successfully posted a message = {} into msgQ. Status={}".format(filename, status)
            self.redis_instance.write_an_event_in_redis_db(event)
            self.redis_instance.increment_enqueue_count()

        return status

    def dequeue(self):
        """
        This method tries to post a message.
        :return Freezes the current context and yeilds a message:
        Please make sure to iterate this over to unfreeze the context.
        """
        if self.message_queue_instance is None:
            self.__connect()
        msg = None
        if hasattr(self.message_queue_instance, 'dequeue'):
            msg = self.message_queue_instance.dequeue()
            if msg:
                self.redis_instance.increment_dequeue_count()
                self.redis_instance.write_an_event_in_redis_db("Consumer {}: Dequeued Message = {}"
                                                               .format(self.thread_identifier,
                                                                       msg))
                self.cleanup()
        return msg

    def cleanup(self):
        if self.message_queue_instance:
            self.message_queue_instance.cleanup()
            self.message_queue_instance = None
