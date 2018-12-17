from confluent_kafka import Producer, Consumer, KafkaException, admin
import os
import sys, traceback
import time
import json
import logging
from pprint import pformat


# sys.path.append("..")  # Adds higher directory to python modules path.

def import_all_packages():
    realpath = os.path.realpath(__file__)
    # print("os.path.realpath({})={}".format(__file__,realpath))
    dirname = os.path.dirname(realpath)
    # print("os.path.dirname({})={}".format(realpath,dirname))
    dirname_list = dirname.split('/')
    # print(dirname_list)
    for index in range(len(dirname_list)):
        module_path = '/'.join(dirname_list[:index])
        # print("module_path={}".format(module_path))
        try:
            sys.path.append(module_path)
        except:
            # print("Invalid module path {}".format(module_path))
            pass


import_all_packages()
from infrastructure_components.log.log_file import logging_to_console_and_syslog


def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


def print_assignment(consumer, partitions):
    logging_to_console_and_syslog('consumer = {}, Assignment {}:'.format(consumer, partitions))


class ConfluentKafkaMsgQAPI:
    """
    This class provides API's into interact with Kafka Queue.
    """

    def __init__(self,
                 is_producer=False,
                 is_consumer=False,
                 perform_subscription=False,
                 thread_identifier=None):
        if not is_producer and not is_consumer:
            logging_to_console_and_syslog("ConfluentKafkaMsgQAPI: You need to pick either producer or consumer.")
            pass
        self.producer_instance = None
        self.consumer_instance = None
        self.broker_name = None
        self.topic = None
        self.producer_conf = None
        self.consumer_conf = None
        self.is_topic_created = False
        self.perform_subscription = perform_subscription
        self.thread_identifier = thread_identifier
        self.__read_environment_variables()
        # if is_producer:
        #    self.__producer_connect()
        # if is_consumer:
        #    self.__consumer_connect()

    def __read_environment_variables(self):
        """
        This method is used to read the environment variables defined in the OS.
        :return:
        """
        while self.broker_name is None or \
                self.topic is None:
            time.sleep(2)
            logging_to_console_and_syslog("ConfluentKafkaMsgQAPI: "
                                          "Trying to read the environment variables...")
            self.broker_name = os.getenv("broker_name_key", default=None)
            self.topic = os.getenv("topic_key", default=None)
        logging_to_console_and_syslog("ConfluentKafkaMsgQAPI: broker_name={}".format(self.broker_name))
        logging_to_console_and_syslog("ConfluentKafkaMsgQAPI: topic={}".format(self.topic))

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    @staticmethod
    def delivery_callback(err, msg):
        if err:
            logging_to_console_and_syslog('%% Message failed delivery: %s\n' % err)
        else:
            logging_to_console_and_syslog('%% Message delivered to %s [%d] @ %s\n' %
                                          (msg.topic(), msg.partition(), str(msg.offset())))

    def __producer_connect(self):
        """
        This method tries to connect to the kafka broker based upon the type of kafka.
        :return:
        """
        is_connected = False
        if self.producer_instance is None:
            try:
                self.producer_conf = {'bootstrap.servers': self.broker_name}
                # Create Producer instance
                self.producer_instance = Producer(**self.producer_conf)
                is_connected = True
            except:
                print("Exception in user code:")
                print("-" * 60)
                traceback.print_exc(file=sys.stdout)
                print("-" * 60)
                time.sleep(5)
            else:
                logging_to_console_and_syslog("ConfluentKafkaMsgQAPI: Successfully "
                                              "connected to broker_name={}"
                                              .format(self.broker_name))
        return is_connected

    def enqueue(self, filename):
        """
        This method tries to post a message to the pre-defined kafka topic.
        :param filename:
        :return status False or True:
        """
        status = False

        if filename is None or len(filename) == 0:
            logging_to_console_and_syslog("ConfluentKafkaMsgQAPI: filename is None or invalid")
            return status
        if self.producer_instance is None:
            logging_to_console_and_syslog("KafkaMsgQAPI: Producer instance is None. Trying to create one..")
            if not self.__producer_connect():
                logging_to_console_and_syslog("Unable to create producer instance.")
                return status

        if not self.is_topic_created:
            try:
                if self.producer_instance.list_topics(self.topic,
                                                      timeout=1.0):
                    logging_to_console_and_syslog("Found topic name = {} in the zookeeper."
                                                  .format(self.topic))
                    self.is_topic_created = True
            except KafkaException:
                kafka_admin_client = admin.AdminClient(self.producer_conf)
                logging_to_console_and_syslog("Creating topic {}."
                                              .format(self.topic))
                ret = kafka_admin_client.create_topics(new_topics=[admin.NewTopic(topic=self.topic,
                                                                                  num_partitions=1)],
                                                       operation_timeout=1.0)
                logging_to_console_and_syslog("ret = {}".format(ret))

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        logging_to_console_and_syslog(
            "ConfluentKafkaMsgQAPI: Posting filename={} into "
            "kafka broker={}, topic={}"
                .format(filename,
                        self.broker_name,
                        self.topic))
        value = filename.encode('utf-8')
        try:
            # Produce line (without newline)
            self.producer_instance.produce(self.topic,
                                           value,
                                           callback=ConfluentKafkaMsgQAPI.delivery_callback)
            status = True
        except BufferError:
            sys.stderr.write('%% Local producer queue is full '
                             '(%d messages awaiting delivery): try again\n' %
                             len(self.producer_instance))
            status = False
        except:
            print("ConfluentKafkaMsgQAPI: Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
            status = False
        else:
            event = "ConfluentKafkaMsgQAPI: Posting filename={} into " \
                    "kafka broker={}, topic={}." \
                .format(filename,
                        self.broker_name,
                        self.topic)
            logging_to_console_and_syslog(event)
            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            # Serve delivery callback queue.
            # NOTE: Since produce() is an asynchronous API this poll() call
            #       will most likely not serve the delivery callback for the
            #       last produce()d message.
            self.producer_instance.poll(timeout=0.1)
            # Wait until all messages have been delivered
            # sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer_instance))
            self.producer_instance.flush(timeout=0.1)

            return status

    def __consumer_connect_to_broker(self):
        """
        This method tries to connect to the kafka broker.
        :return:
        """
        is_connected = False

        # Consumer configuration
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        """
            self.consumer_conf = {'bootstrap.servers': self.broker_name,
                              'group.id': 'kafka-consumer',
                              'session.timeout.ms': 6000,
                              'auto.offset.reset': 'earliest'}
        """
        if self.consumer_instance is None:
            try:

                logging_to_console_and_syslog("Consumer:{}:Trying to connect to broker_name={}"
                                              .format(self.thread_identifier,
                                                      self.broker_name))
                # Create Consumer instance
                # Hint: try debug='fetch' to generate some log messages
                consumer_conf = {'bootstrap.servers': self.broker_name,
                                 'group.id': self.topic,
                                 'session.timeout.ms': 6000,
                                 'auto.offset.reset': 'earliest'}

                # consumer_conf['stats_cb'] = stats_cb
                # consumer_conf['statistics.interval.ms'] = 0
                self.consumer_instance = Consumer(consumer_conf)
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

    @staticmethod
    def print_assignment(consumer, partitions):
        print('consumer = {}, Assignment {}:'.format(consumer, partitions))

    def dequeue(self):
        conf = {'bootstrap.servers': self.broker_name, 'group.id': self.topic, 'session.timeout.ms': 6000,
                'auto.offset.reset': 'earliest'}
        if not self.consumer_instance:
            self.consumer_instance = Consumer(conf)
            self.consumer_instance.subscribe([self.topic], on_assign=ConfluentKafkaMsgQAPI.print_assignment)
        msg = self.consumer_instance.poll(timeout=1.0)
        if msg is None or msg.error():
            return None
        else:
            logging_to_console_and_syslog('%% %s [%d] at offset %d with key %s:\n' %
                                          (msg.topic(), msg.partition(), msg.offset(),
                                           str(msg.key())))
            msg = msg.value().decode('utf8')
            logging_to_console_and_syslog("msg.value()={}".format(msg))
            self.consumer_instance.close()
            self.consumer_instance = None
            return msg

    def cleanup(self):
        if self.consumer_instance:
            self.consumer_instance.close()
            self.consumer_instance = None
