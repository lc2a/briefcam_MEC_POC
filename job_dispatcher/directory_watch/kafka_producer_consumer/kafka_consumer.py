from kafka import KafkaConsumer
import os
import sys, traceback

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
from redisClient.RedisClient import RedisClient
import time
import datetime


class Consumer:
    """
    This class does the following:
    1. It instantiates KAFKA consumer instance.
    2. It consumes a message from a pre-defined topic.
    There are two variants of Kafka that is supported by this class.
    a. wurstmeister KAFKA APIs.
    b. confluentinc KAFKA APIs.
    """

    def __init__(self):
        self.consumer_instance = None
        self.broker_name = None
        self.topic = None
        self.total_job_done_count_redis_name = None
        self.redis_log_keyname = None
        self.redis_instance = RedisClient()
        self.read_environment_variables()
        self.connect_to_kafka_broker()

    def read_environment_variables(self):
        """
        This method is used to read the environment variables defined in the OS.
        :return:
        """
        while self.broker_name is None or \
                self.topic is None or \
                self.redis_log_keyname is None or \
                self.total_job_done_count_redis_name is None:
            time.sleep(2)
            logging_to_console_and_syslog("Kafka Consumer: Trying to read the environment variables...")
            self.broker_name = os.getenv("broker_name_key", default=None)
            self.topic = os.getenv("topic_key", default=None)
            self.redis_log_keyname = os.getenv("redis_log_keyname_key",
                                               default=None)
            self.total_job_done_count_redis_name = os.getenv("total_consumed_count_redis_name_key",
                                                             default=None)
        logging_to_console_and_syslog("broker_name={}".format(self.broker_name))
        logging_to_console_and_syslog("topic={}".format(self.topic))
        logging_to_console_and_syslog("redis_log_keyname={}".format(self.redis_log_keyname))
        logging_to_console_and_syslog("total_produced_count_redis_name={}".format(self.total_job_done_count_redis_name))


    def connect_to_kafka_broker(self):
        """
        This method tries to connect to the kafka broker.
        :return:
        """
        while self.consumer_instance is None:
            try:
                self.consumer_instance = KafkaConsumer(bootstrap_servers=self.broker_name,
                                                       group_id="kafka-consumer")
                time.sleep(5)
            except:
                print("Exception in user code:")
                print("-" * 60)
                traceback.print_exc(file=sys.stdout)
                print("-" * 60)
                time.sleep(5)
        logging_to_console_and_syslog("Successfully connected to broker_name={}".format(self.broker_name))

    def connect_to_kafka_broker_and_to_a_topic(self):
        """
        This method tries to connect to the kafka broker.
        :return:
        """
        while self.consumer_instance is None:
            try:
                self.consumer_instance = KafkaConsumer(self.topic,
                                                       bootstrap_servers=self.broker_name,
                                                       group_id="kafka-consumer")
                time.sleep(5)
            except:
                print("Exception in user code:")
                print("-" * 60)
                traceback.print_exc(file=sys.stdout)
                print("-" * 60)
                time.sleep(5)
        logging_to_console_and_syslog("Successfully connected to broker_name={},topic={}.".format(self.broker_name,
                                                                                                  self.topic))


    def get_current_job_count(self):
        return self.redis_instance.read_key_value_from_redis_db(self.total_job_done_count_redis_name)

    def increment_job_consumed_count(self):
        self.redis_instance.increment_key_in_redis_db(self.total_job_done_count_redis_name)

    def write_an_event_in_redis_db(self, event):
        self.redis_instance.write_an_event_on_redis_db(event, self.redis_log_keyname)

    def iterate_over_kafka_consumer_instance_messages(self):
        logging_to_console_and_syslog("polling for new messages in topic {}.".format(self.topic))
        for msg in self.consumer_instance:
            filename = msg.value.decode('utf-8')
            logging_to_console_and_syslog("Dequeued message {}.".format(filename))
            self.redis_instance.write_an_event_on_redis_db('Processing {}'.format(filename))
            self.increment_job_consumed_count()

    def poll_for_new_messages(self):
        msgs = self.consumer_instance.poll(timeout_ms=100, max_records=1)
        for msg in msgs.values():
            logging_to_console_and_syslog('Received message: {}'.format(repr(msgs)))
            logging_to_console_and_syslog('msg: {}'.format(repr(msg)))

    def subscribe_to_a_topic(self):
        self.consumer_instance.subscribe([self.topic])
        if self.topic in self.consumer_instance.subscription():
            logging_to_console_and_syslog("Found the topic {} in the subscription.".format(
                self.topic))
            return True
        return False

    def connect_and_poll_for_new_message(self):
        status = False
        try:
            #self.connect_to_kafka_broker()
            #self.subscribe_to_a_topic()
            #self.iterate_over_kafka_consumer_instance_messages()
            self.connect_to_kafka_broker_and_to_a_topic()
            for index in range(10):
                time.sleep(1)
                self.poll_for_new_messages()

            status = True
        except:
            logging_to_console_and_syslog(
                "Exception occurred while polling for "
                "a message from kafka Queue. {} ".format(sys.exc_info()[0]))

            print("Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
        finally:
            self.cleanup()
        return status

    def cleanup(self):
        self.consumer_instance.close()
        self.consumer_instance = None
