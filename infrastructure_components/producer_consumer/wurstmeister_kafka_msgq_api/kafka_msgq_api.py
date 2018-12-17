from kafka import KafkaProducer, KafkaConsumer
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

class KafkaMsgQAPI:
    """
    This class provides API's into interact with Kafka Queue.
    """
    def __init__(self,
                 is_producer=False,
                 is_consumer=False,
                 perform_subscription=False,
                 thread_identifier=None):
        if not is_producer and not is_consumer:
            logging_to_console_and_syslog("KafkaMsgQAPI: You need to pick either producer or consumer.")
            pass
        self.producer_instance = None
        self.consumer_instance = None
        self.broker_name = None
        self.topic = None
        self.perform_subscription = perform_subscription
        self.thread_identifier = thread_identifier
        self.__read_environment_variables()
        #if is_producer:
        #    self.__producer_connect()
        #if is_consumer:
        #    self.__consumer_connect()

    def __read_environment_variables(self):
        """
        This method is used to read the environment variables defined in the OS.
        :return:
        """
        while self.broker_name is None or \
                self.topic is None:
            time.sleep(2)
            logging_to_console_and_syslog("KafkaMsgQAPI: "
                                          "Trying to read the environment variables...")
            self.broker_name = os.getenv("broker_name_key", default=None)
            self.topic = os.getenv("topic_key", default=None)
        logging_to_console_and_syslog("Consumer{}:KafkaMsgQAPI: broker_name={}"
                                      .format(self.thread_identifier,
                                              self.broker_name))
        logging_to_console_and_syslog("Consumer{}:KafkaMsgQAPI: topic={}"
                                      .format(self.thread_identifier,
                                              self.topic))
        logging_to_console_and_syslog("Consumer{}:self.perform_subscription={}"
                                      .format(self.thread_identifier,
                                              self.perform_subscription))

    def __producer_connect(self):
        """
        This method tries to connect to the kafka broker based upon the type of kafka.
        :return:
        """
        is_connected = False
        if self.producer_instance is None:
            try:
                self.producer_instance = KafkaProducer(bootstrap_servers=self.broker_name,
                                                       acks=0)
                is_connected = True
            except:
                print("Exception in user code:")
                print("-" * 60)
                traceback.print_exc(file=sys.stdout)
                print("-" * 60)
                time.sleep(5)
            else:
                logging_to_console_and_syslog("KafkaMsgQAPI: Successfully "
                                              "connected to broker_name={}"
                                              .format(self.broker_name))
        return is_connected

    def __consumer_connect(self):
        status = False
        try:
            if self.perform_subscription:
                if self.__consumer_connect_to_broker():
                    status = self.__subscribe_to_a_topic()
                #self.__iterate_over_kafka_consumer_instance_messages()
            else:
                status = self.__consumer_connect_to_kafka_broker_and_to_a_topic()
                #self.__consumer_poll_for_new_messages()
        except:
            logging_to_console_and_syslog(
                "{}:Exception occurred while polling for "
                "a message from kafka Queue. {} "
                .format(self.thread_identifier,
                        sys.exc_info()[0]))

            print("{}:Exception in user code:".format(self.thread_identifier))
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
        return status

    def enqueue(self, filename):
        """
        This method tries to post a message to the pre-defined kafka topic.
        :param filename:
        :return status False or True:
        """
        status = False

        if filename is None or len(filename) == 0:
            logging_to_console_and_syslog("KafkaMsgQAPI: filename is None or invalid")
            return status
        if self.producer_instance is None:
            logging_to_console_and_syslog("KafkaMsgQAPI: Producer instance is None. Trying to create one..")
            if not self.__producer_connect():
                logging_to_console_and_syslog("Unable to create producer instance.")
                return status

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        logging_to_console_and_syslog(
            "KafkaMsgQAPI: Posting filename={} into "
            "kafka broker={}, topic={}"
            .format(filename,
                    self.broker_name,
                    self.topic))
        value = filename.encode('utf-8')
        try:
            future = self.producer_instance.send(self.topic, value)
            result = future.get(timeout=60)
            status = True
        except:
            print("KafkaMsgQAPI: Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
        else:
            event = "KafkaMsgQAPI: Posting filename={} into " \
                    "kafka broker={}, topic={}, result = {}" \
                .format(filename,
                        self.broker_name,
                        self.topic,
                        result)
            logging_to_console_and_syslog(event)
            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            #self.producer_instance.flush()
            return status

    def __consumer_connect_to_kafka_broker_and_to_a_topic(self):
        """
        This method tries to connect to the kafka broker.
        :return:
        """
        is_connected = False
        if self.consumer_instance is None:
            try:
                logging_to_console_and_syslog("Consumer:{}:Trying to connect to broker_name={}"
                                              .format(self.thread_identifier,
                                                      self.broker_name))

                self.consumer_instance = KafkaConsumer(self.topic,
                                                       bootstrap_servers=self.broker_name,
                                                       group_id="kafka-consumer")
                is_connected = True
            except:
                print("Consumer:{}:Exception in user code:"
                      .format(self.thread_identifier))
                print("-" * 60)
                traceback.print_exc(file=sys.stdout)
                print("-" * 60)
                time.sleep(5)

        logging_to_console_and_syslog("Consumer:{}:Successfully connected to "
                                      "broker_name={},topic={}."
                                      .format(self.thread_identifier,
                                              self.broker_name,
                                              self.topic))
        return is_connected

    def __consumer_poll_for_new_messages(self):
        """
        logging_to_console_and_syslog("{}: Polling the kafka consumer instance for "
                                      "new messages in the topic {}."
                                      .format(self.thread_identifier, self.topic))
        """

        msgs = self.consumer_instance.poll(timeout_ms=100,
                                           max_records=1)
        for msg in msgs.values():
            logging_to_console_and_syslog('Consumer:{}:Received message: {}'
                                          .format(self.thread_identifier,
                                                  repr(msgs)))
            logging_to_console_and_syslog('Consumer:{}:msg: {}'
                                          .format(self.thread_identifier,
                                                  repr(msg)))
            self.cleanup()
            return msg[0].value.decode('utf-8')

    def __consumer_connect_to_broker(self):
        """
        This method tries to connect to the kafka broker.
        :return:
        """
        is_connected = False
        if self.consumer_instance is None:
            try:

                logging_to_console_and_syslog("Consumer:{}:Trying to connect to broker_name={}"
                                              .format(self.thread_identifier,
                                                      self.broker_name))

                self.consumer_instance = KafkaConsumer(bootstrap_servers=self.broker_name,
                                                       group_id="kafka-consumer")
                is_connected = True
            except:
                logging_to_console_and_syslog("Consumer:{}:Exception in user code:"
                                              .format(self.thread_identifier))
                logging_to_console_and_syslog("-" * 60)
                traceback.print_exc(file=sys.stdout)
                logging_to_console_and_syslog("-" * 60)
                time.sleep(5)

        logging_to_console_and_syslog("Consumer:{}:Consumer Successfully "
                                      "connected to broker_name={}"
                                      .format(self.thread_identifier,
                                              self.broker_name))
        return is_connected

    def __subscribe_to_a_topic(self):
        is_subscription_successful = False
        try:
            if self.topic in self.consumer_instance.subscription():
                logging_to_console_and_syslog("Consumer:{}: Found the topic {} "
                                              "in the subscription."
                                              .format(self.thread_identifier,
                                                      self.topic))
                is_subscription_successful = True
        except:
            try:
                self.consumer_instance.subscribe([self.topic])
                is_subscription_successful = True
                logging_to_console_and_syslog("Consumer:{}: Subscribed to topic {}."
                                              .format(self.thread_identifier,
                                                      self.topic))
            except:
                logging_to_console_and_syslog("Consumer:{}: Caught an exception while trying to subscribe to topic {}."
                                              .format(self.thread_identifier,
                                                      self.topic))
        return is_subscription_successful

    def __iterate_over_kafka_consumer_instance_messages(self):
        """
        logging_to_console_and_syslog("Consumer:{}: dequeue {}."
                                      .format(self.thread_identifier,
                                           self.topic))
        """
        for msg in self.consumer_instance:
            filename = msg.value.decode('utf-8')
            logging_to_console_and_syslog("Consumer:{}: Dequeued message {}."
                                          .format(self.thread_identifier,
                                                  filename))
            yield msg

    def dequeue(self):
        try:
            if not self.consumer_instance:
                logging_to_console_and_syslog("Consumer instance is None...Creating a new consumer instance.")
                if not self.__consumer_connect():
                    logging_to_console_and_syslog("Unable to create a consumer instance..")
                    return None

            if self.perform_subscription:
                #logging_to_console_and_syslog("{}:Perform __iterate_over_kafka_consumer_instance_messages."
                #                             .format(self.thread_identifier))
                #return self.__iterate_over_kafka_consumer_instance_messages()
                return self.__consumer_poll_for_new_messages()
            else:
                #logging_to_console_and_syslog("{}:Perform __consumer_poll_for_new_messages."
                #                              .format(self.thread_identifier))
                return self.__consumer_poll_for_new_messages()
        except:
            logging_to_console_and_syslog(
                "KafkaMsgQAPI:Exception occurred while polling for "
                "a message from kafka Queue. {} ".format(sys.exc_info()[0]))

            logging_to_console_and_syslog("KafkaMsgQAPI:Exception in user code:")
            logging_to_console_and_syslog("-" * 60)
            traceback.print_exc(file=sys.stdout)
            logging_to_console_and_syslog("-" * 60)

        return None

    def cleanup(self):
        if self.producer_instance:
            self.producer_instance.close()
            self.producer_instance = None
        if self.consumer_instance:
            self.consumer_instance.close()
            self.consumer_instance = None
