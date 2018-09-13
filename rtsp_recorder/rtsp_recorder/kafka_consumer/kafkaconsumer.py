#!/usr/bin/env python3
import time
from kafka import KafkaConsumer
import os
import logging
import sys
import traceback
sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog

def on_assign_partition_to_subscriber(consumer, partitions):
    logging_to_console_and_syslog("partition {} is "
                                  "assigned to the consumer {}"
                                  .format(str(partitions), str(consumer)),
                                  logging.INFO)
class KafkaConsumer:
    def __init__(self):
        self.broker_name = None
        self.topic = None
        self.consumer_instance = None
        self.hostname = os.popen("cat /etc/hostname").read()
        self.cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()
        self.load_environment_variables()
        self.connect_to_kafka_broker()

    def load_environment_variables(self):
        while self.broker_name is None and \
                self.topic is None:
            time.sleep(2)
            logging_to_console_and_syslog("Trying to read the "
                                          "environment variables...")
            self.topic = os.getenv("topic_key",
                                   default=None)
            self.broker_name = os.getenv("broker_name_key",
                                         default=None)

        logging_to_console_and_syslog("broker_name={}"
                                      .format(self.broker_name),
                                      logging.INFO)
        logging_to_console_and_syslog("topic={}"
                                      .format(self.topic),
                                      logging.INFO)

    def connect_to_kafka_broker(self):
        self.consumer_instance = None
        while self.consumer_instance is None:
            self.consumer_instance = KafkaConsumer(bootstrap_servers=self.broker_name,
                                                   group_id="kafka-consumer")
            time.sleep(5)
        logging_to_console_and_syslog('Successfully '
                                      'attached to bootstrap server={},'
                                      .format(self.broker_name),
                                      logging.INFO)

    def close_kafka_instance(self):
        if self.consumer_instance:
            self.consumer_instance.close()
            self.consumer_instance = None

    def connect_and_poll_for_new_message(self):
        message = None
        try:
            self.connect_to_kafka_broker()
            continue_inner_poll = True
            self.consumer_instance.subscribe([self.topic])
            while continue_inner_poll:
                for msg in self.consumer_instance:
                    message = msg.value.decode('utf-8')
                    continue_inner_poll=False
        except KeyboardInterrupt:
            logging_to_console_and_syslog("Keyboard interrupt. {}".format(sys.exc_info()[0]))
            raise KeyboardInterrupt
        except:
            logging_to_console_and_syslog(
                "Exception occurred while polling for "
                "a message from kafka Queue. {} ".format(sys.exc_info()[0]))

            print("Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
        finally:
            return message
