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


class Consumer:
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
            self.consumer_instance = KafkaConsumer(self.topic,
                                                   bootstrap_servers=self.broker_name,
                                                   group_id=self.cont_id[:13])
            time.sleep(5)
        logging_to_console_and_syslog('Successfully '
                                      'attached to bootstrap server={} and '
                                      'subscribed to topic {}.,'
                                      .format(self.broker_name,
                                              self.topic),
                                      logging.INFO)

    def close_kafka_instance(self):
        if self.consumer_instance:
            self.consumer_instance.close()
            self.consumer_instance = None

    @property
    def poll_for_new_message(self):
        try:
            continue_inner_poll = True
            logging_to_console_and_syslog("Trying to read messages from the kafkaQ...")
            while continue_inner_poll is True:
                msg = self.consumer_instance.poll(timeout_ms=100, max_records=1)
                for key, record in msg.items():
                    for elements in record:
                        decoded_str = elements.value.decode('utf-8')
                        logging_to_console_and_syslog("Message={},record.timestamp={},record.value={}"
                                    .format(decoded_str,
                                            elements.timestamp,
                                            elements.value))
                        return decoded_str

        except KeyboardInterrupt:
            logging_to_console_and_syslog("Keyboard interrupt. {}".format(sys.exc_info()[0]))
            raise KeyboardInterrupt
        except:
            logging_to_console_and_syslog(
                "Exception occurred while polling for "
                "a message from kafka Queue. {} {} "
                    .format(sys.exc_info()[0], repr(msg)))

            print("Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
        return None
