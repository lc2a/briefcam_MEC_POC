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
        if is_producer:
            self.__producer_connect()
        if is_consumer:
            self.__consumer_connect()

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
        while self.producer_instance is None:
            try:
                self.producer_conf = {'bootstrap.servers': self.broker_name}
                # Create Producer instance
                self.producer_instance = Producer(**self.producer_conf)
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

    def __consumer_connect(self):
        status = False
        try:
            if self.perform_subscription:
                self.__consumer_connect_to_broker()
                self.__subscribe_to_a_topic()
                # self.__iterate_over_kafka_consumer_instance_messages()
            else:
                self.__consumer_connect_to_kafka_broker_and_to_a_topic()
                # self.__consumer_poll_for_new_messages()
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
        :return status False or True:
        """
        status = False

        if filename is None or len(filename) == 0:
            logging_to_console_and_syslog("ConfluentKafkaMsgQAPI: filename is None or invalid")
            return status
        if self.producer_instance is None:
            logging_to_console_and_syslog("ConfluentKafkaMsgQAPI: instance is None")
            return status

        if not self.is_topic_created:
            try:
                if self.producer_instance.list_topics(self.topic,
                                                      timeout=1.0):
                    logging_to_console_and_syslog("Found topic name = {} in the zookeeper."
                                                  .format(self.topic))
                    self.is_topic_created = True
            except KafkaException:
                self.kafka_admin_client = admin.AdminClient(self.producer_conf)
                logging_to_console_and_syslog("Creating topic {}."
                                              .format(self.topic))
                ret = self.kafka_admin_client.create_topics(new_topics=[admin.NewTopic(topic=self.topic,
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

    def __consumer_connect_to_kafka_broker_and_to_a_topic(self):
        """
        This method tries to connect to the kafka broker.
        :return:
        """
        pass

    def __consumer_poll_for_new_messages(self):

        logging_to_console_and_syslog("{}: Polling the kafka consumer instance for "
                                      "new messages in the topic {}."
                                      .format(self.thread_identifier, self.topic))
        # Read messages from Kafka, print to stdout
        try:
            while True:
                msg = self.consumer_instance.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    # Proper message
                    sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                     (msg.topic(), msg.partition(), msg.offset(),
                                      str(msg.key())))
                    print(msg.value())

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

        finally:
            # Close down consumer to commit final offsets.
            self.consumer_instance.close()

        """
        msg = self.consumer_instance.poll(timeout=5.0)
        if msg is None:
            return None

        if msg.error():
            raise KafkaException(msg.error())
        else:
            logging_to_console_and_syslog("msg = {}".format(msg))

            logging_to_console_and_syslog('Consumer:{}: Rcvd msg %% %s [%d] at offset %d with key %s: value : %s\n'
                                          .format(self.thread_identifier,
                                                  msg.topic(),
                                                  msg.partition(),
                                                  msg.offset(),
                                                  str(msg.key()),
                                                  str(msg.value()))
                                          )
        return msg.value()
        """
        return None

    def __consumer_connect_to_broker(self):
        """
        This method tries to connect to the kafka broker.
        :return:
        """
        if self.consumer_instance:
            return

        # Consumer configuration
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        """
            self.consumer_conf = {'bootstrap.servers': self.broker_name,
                              'group.id': 'kafka-consumer{}'.format(self.thread_identifier),
                              'session.timeout.ms': 6000,
                              'auto.offset.reset': 'earliest'}
        """
        consumer_conf = {'bootstrap.servers': self.broker_name, 'group.id': 'group', 'session.timeout.ms': 6000,
                'auto.offset.reset': 'earliest'}
        consumer_conf['stats_cb'] = stats_cb
        consumer_conf['statistics.interval.ms'] = 0

        # Create logger for consumer (logs will be emitted when poll() is called)
        logger = logging.getLogger('consumer')
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
        logger.addHandler(handler)

        while self.consumer_instance is None:
            try:

                logging_to_console_and_syslog("Consumer:{}:Trying to connect to broker_name={}"
                                              .format(self.thread_identifier,
                                                      self.broker_name))
                # Create Consumer instance
                # Hint: try debug='fetch' to generate some log messages
                self.consumer_instance = Consumer(consumer_conf, logger=logger)
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

    @staticmethod
    def print_assignment(consumer, partitions):
        logging_to_console_and_syslog('consumer = {}, Assignment {}:', repr(consumer), partitions)

    def __subscribe_to_a_topic(self):
        try:
            # Subscribe to topics
            cluster_meta_data = self.consumer_instance.list_topics(self.topic, timeout=0.3)
            logging_to_console_and_syslog("ClusterMetaData={}".format(repr(cluster_meta_data)))
            if self.topic not in cluster_meta_data.topics.keys():
                logging_to_console_and_syslog("Topic {} is "
                                              "not found in the ClusterMetaData {}"
                                              .format(self.topic,
                                                      repr(cluster_meta_data.topics.keys())))
                raise KafkaException

            def print_assignment(consumer, partitions):
                print('Assignment:', partitions)

            # Subscribe to topics
            self.consumer_instance.subscribe(self.topics, on_assign=print_assignment)

            """
            self.consumer_instance.subscribe(self.topic,
                                             on_assign=ConfluentKafkaMsgQAPI.print_assignment)
            """
        except:
            logging_to_console_and_syslog("Consumer:{}: Subscribed to topic {}."
                                          .format(self.thread_identifier,
                                                  self.topic))
        return True

    def __iterate_over_kafka_consumer_instance_messages(self):
        """
        logging_to_console_and_syslog("Consumer:{}: dequeue {}."
                                      .format(self.thread_identifier,
                                           self.topic))
        """
        pass

    def dequeue(self):
        try:
            if self.perform_subscription:
                # logging_to_console_and_syslog("{}:Perform __consumer_poll_for_new_messages."
                #                              .format(self.thread_identifier))
                return self.__consumer_poll_for_new_messages()
            else:
                # logging_to_console_and_syslog("{}:Perform __iterate_over_kafka_consumer_instance_messages."
                #                             .format(self.thread_identifier))
                return self.__iterate_over_kafka_consumer_instance_messages()

        except:
            logging_to_console_and_syslog(
                "ConfluentKafkaMsgQAPI:Exception occurred while polling for "
                "a message from kafka Queue. {} ".format(sys.exc_info()[0]))

            logging_to_console_and_syslog("ConfluentKafkaMsgQAPI:Exception in user code:")
            logging_to_console_and_syslog("-" * 60)
            traceback.print_exc(file=sys.stdout)
            logging_to_console_and_syslog("-" * 60)

        return None

    def cleanup(self):
        pass
