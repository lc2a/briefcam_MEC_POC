from kafka import KafkaProducer
import os
import sys, traceback

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
import time
from redisClient.RedisClient import RedisClient


class Producer:
    """
    This class does the following:
    1. It instantiates KAFKA producer instance.
    2. It posts a message to a pre-defined topic.
    There are two variants of Kafka that is supported by this class.
    a. wurstmeister KAFKA APIs.
    b. confluentinc KAFKA APIs.
    """
    def __init__(self):
        self.producer_instance = None
        self.broker_name = None
        self.topic = None
        self.total_job_done_count_redis_name = None
        self.redis_log_keyname = None
        self.read_environment_variables()
        self.connect_to_kafka_broker()
        self.redis_instance = RedisClient()

    def read_environment_variables(self):
        """
        This method is used to read the environment variables defined in the OS.
        :return:
        """
        while self.broker_name is None or \
                self.topic is None or \
                self.redis_log_keyname is None or \
                self.total_job_done_count_redis_name is None :
            time.sleep(2)
            logging_to_console_and_syslog("Kafka Producer: Trying to read the environment variables...")
            self.broker_name = os.getenv("broker_name_key", default=None)
            self.topic = os.getenv("topic_key", default=None)
            self.redis_log_keyname = os.getenv("redis_log_keyname_key",
                                           default=None)
            self.total_job_done_count_redis_name = os.getenv("total_produced_count_redis_name_key",
                                                         default=None)
        logging_to_console_and_syslog("broker_name={}".format(self.broker_name))
        logging_to_console_and_syslog("topic={}".format(self.topic))
        logging_to_console_and_syslog("redis_log_keyname={}".format(self.redis_log_keyname))
        logging_to_console_and_syslog("total_produced_count_redis_name={}".format(self.total_job_done_count_redis_name))

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
                logging_to_console_and_syslog("Successfully connected to broker_name={}".format(self.broker_name))

    def get_current_job_count(self):
        return self.redis_instance.read_key_value_from_redis_db(self.total_job_done_count_redis_name)

    def increment_job_produced_count(self):
        self.redis_instance.increment_key_in_redis_db(self.total_job_done_count_redis_name)

    def write_an_event_in_redis_db(self, event):
        self.redis_instance.write_an_event_on_redis_db(event, self.redis_log_keyname)

    def post_message_to_a_kafka_topic(self, filename):
        """
        This method tries to post a message to the pre-defined kafka topic.
        :param filename:
        :return:
        """
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
        result = None
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
            self.write_an_event_in_redis_db(event)
            self.increment_job_produced_count()
            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            self.producer_instance.flush()

    def cleanup(self):
        self.producer_instance.close()