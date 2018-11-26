from kafka import KafkaProducer, KafkaConsumer
import os
import sys, traceback

#sys.path.append("..")  # Adds higher directory to python modules path.
import time
sys.path.append("...")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
from redisClient.redis_interface import RedisInterface


class KafkaMsgQAPI:
    """
    This class provides API's into interact with Kafka Queue.
    """
    def __init__(self,
                 isProducer=False,
                 isConsumer=False,
                 thread_identifier=None):
        if not isProducer and not isConsumer:
            pass
        self.producer_instance = None
        self.consumer_instance = None
        self.broker_name = None
        self.topic = None
        self.perform_subscription = True
        self.thread_identifier = thread_identifier
        self.__read_environment_variables()
        self.redis_instance = RedisInterface()
        if isProducer:
            self.__producer_connect()
        elif isConsumer:
            self.__consumer_connect()

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
        logging_to_console_and_syslog("KafkaMsgQAPI: broker_name={}".format(self.broker_name))
        logging_to_console_and_syslog("KafkaMsgQAPI: topic={}".format(self.topic))

    def __producer_connect(self):
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
                logging_to_console_and_syslog("KafkaMsgQAPI: Successfully "
                                              "connected to broker_name={}"
                                              .format(self.broker_name))

    def __consumer_connect(self):
        status = False
        try:
            if self.perform_subscription:
                self.__consumer_connect_to_broker()
                self.__subscribe_to_a_topic()
                self.__iterate_over_kafka_consumer_instance_messages()
            else:
                self.__consumer_connect_to_kafka_broker_and_to_a_topic()
                self.__consumer_poll_for_new_messages()
            status = True
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
        :return:
        """
        if filename is None or len(filename) == 0:
            logging_to_console_and_syslog("KafkaMsgQAPI: filename is None or invalid")
            return
        if self.producer_instance is None:
            logging_to_console_and_syslog("KafkaMsgQAPI: instance is None")
            return

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
            self.redis_instance.write_an_event_in_redis_db(event)
            self.redis_instance.increment_enqueue_count()
            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            self.producer_instance.flush()

    def __consumer_connect_to_kafka_broker_and_to_a_topic(self):
        """
        This method tries to connect to the kafka broker.
        :return:
        """
        if self.consumer_instance:
            return

        while self.consumer_instance is None:
            try:
                logging_to_console_and_syslog("{}:Trying to connect to broker_name={}"
                                              .format(self.thread_identifier,
                                                      self.broker_name))

                self.consumer_instance = KafkaConsumer(self.topic,
                                                       bootstrap_servers=self.broker_name,
                                                       group_id="kafka-consumer")
                time.sleep(5)
            except:
                print("{}:Exception in user code:"
                      .format(self.thread_identifier))
                print("-" * 60)
                traceback.print_exc(file=sys.stdout)
                print("-" * 60)
                time.sleep(5)

        logging_to_console_and_syslog("{}:Successfully connected to "
                                      "broker_name={},topic={}."
                                      .format(self.thread_identifier,
                                              self.broker_name,
                                              self.topic))

    def __consumer_poll_for_new_messages(self):
        """
        logging_to_console_and_syslog("{}: Polling the kafka consumer instance for "
                                      "new messages in the topic {}."
                                      .format(self.thread_identifier, self.topic))
        """

        msgs = self.consumer_instance.poll(timeout_ms=100,
                                           max_records=1)
        for msg in msgs.values():
            logging_to_console_and_syslog('{}:Received message: {}'
                                          .format(self.thread_identifier,
                                                  repr(msgs)))
            logging_to_console_and_syslog('{}:msg: {}'
                                          .format(self.thread_identifier,
                                                  repr(msg)))
            yield msg

    def __consumer_connect_to_broker(self):
        """
        This method tries to connect to the kafka broker.
        :return:
        """
        if self.consumer_instance:
            return

        while self.consumer_instance is None:
            try:

                logging_to_console_and_syslog("{}:Trying to connect to broker_name={}"
                                              .format(self.thread_identifier,
                                                      self.broker_name))

                self.consumer_instance = KafkaConsumer(bootstrap_servers=self.broker_name,
                                                       group_id="kafka-consumer")
                time.sleep(1)
            except:
                print("{}:Exception in user code:"
                      .format(self.thread_identifier))
                print("-" * 60)
                traceback.print_exc(file=sys.stdout)
                print("-" * 60)
                time.sleep(5)

        logging_to_console_and_syslog("{}:Consumer Successfully "
                                      "connected to broker_name={}"
                                      .format(self.thread_identifier,
                                              self.broker_name))

    def __subscribe_to_a_topic(self):
        try:
            if self.topic in self.consumer_instance.subscription():
                logging_to_console_and_syslog("{}: Found the topic {} "
                                              "in the subscription."
                                              .format(self.thread_identifier,
                                                      self.topic))
        except:
            self.consumer_instance.subscribe([self.topic])
        return True

    def __iterate_over_kafka_consumer_instance_messages(self):
        logging_to_console_and_syslog("{}: Iterating the "
                                      "kafka consumer instance for "
                                      "new messages in the topic {}."
                                      .format(self.thread_identifier,
                                              self.topic))
        for msg in self.consumer_instance:
            filename = msg.value.decode('utf-8')
            logging_to_console_and_syslog("{}: Dequeued message {}."
                                          .format(self.thread_identifier,
                                                  filename))
            self.redis_instance.write_an_event_on_redis_db('{}: Processing {}'
                                                           .format(self.thread_identifier,
                                                                   filename))
            self.redis_instance.increment_dequeue_count()
            yield msg

    def get_current_enqueue_count(self):
        return self.redis_instance.get_current_enqueue_count()

    def get_current_dequeue_count(self):
        return self.redis_instance.get_current_enqueue_count()

    def dequeue(self):
        msg = None
        try:
            if self.perform_subscription:
                msg = self.__iterate_over_kafka_consumer_instance_messages()
            else:
                msg = self.__consumer_poll_for_new_messages()
        except:
            logging_to_console_and_syslog(
                "KafkaMsgQAPI:Exception occurred while polling for "
                "a message from kafka Queue. {} ".format(sys.exc_info()[0]))

            print("KafkaMsgQAPI:Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
        return msg

    def cleanup(self):
        self.producer_instance.close()
