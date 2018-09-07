#!/usr/bin/env python3

import time

time.sleep(2)

from kafka import KafkaConsumer
import os
import logging
import sys
from datetime import datetime

hostname = os.getenv("hostname", default=None)
cont_id = os.popen("cat /proc/self/cgroup | grep \"cpu:/\" | sed \'s/\([0-9]\):cpu:\/docker\///g\'").read()


def logging_to_console_and_syslog(log):
    logging.debug(log)
    i = datetime.now()
    print(str(i) + " hostname={} containerID={} ".format(hostname, cont_id[:5]) + log)


def on_assign_partition_to_subscriber(consumer, partitions):
    logging_to_console_and_syslog("partition {} is assigned to the consumer {}".format(str(partitions), str(consumer)))


class Poll_for_new_file_name():
    def __init__(self):
        self.broker_name = None
        self.topic = None
        self.consumer_instance = None
        import upload_video.upload_video_to_briefcam
        self.briefcam_obj = None
        logging.basicConfig(format='(%(threadName)-2s:'
                                   '%(levelname)s:'
                                   '%(asctime)s:'
                                   '%(lineno)d:'
                                   '%(filename)s:'
                                   '%(funcName)s:'
                                   '%(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            filename='poll_for_new_filename.log',
                            level=logging.DEBUG)

    def load_environment_variables(self):
        while self.broker_name is None and self.topic is None:
            time.sleep(2)
            logging_to_console_and_syslog("Trying to read the environment variables, broker_name_key and topic_key")
            self.topic = os.getenv("topic_key", default=None)
            self.broker_name = os.getenv("broker_name_key", default=None)
        logging_to_console_and_syslog("broker_name={}".format(self.broker_name))
        logging_to_console_and_syslog("topic={}".format(self.topic))

    def connect_to_kafka_broker(self):
        self.consumer_instance = None
        while self.consumer_instance is None:
            self.consumer_instance = KafkaConsumer(self.topic, bootstrap_servers=self.broker_name,
            group_id='my_consumer_group')
        logging_to_console_and_syslog('Successfully attached to bootstrap server={},'.format(self.broker_name))

    def connect_to_xhost_environment(self):
        connected = False
        sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        # sys.path.append('..\\upload_video.upload_video_to_briefcam')
        while connected == False:
            try:
                from upload_video.upload_video_to_briefcam import upload_video_to_briefcam
            except:
                logging_to_console_and_syslog("Unable to import module upload_video" + sys.exc_info()[0])
            else:
                logging_to_console_and_syslog("successfully connected to xhost display")
                self.briefcam_obj = upload_video_to_briefcam()
                connected = True

    def connect_and_poll_for_new_message(self):
        self.connect_to_kafka_broker()
        try:
            continue_inner_poll = True
            while continue_inner_poll:
                for msg in self.consumer_instance:
                    filename = msg.decode('utf-8')
                    logging_to_console_and_syslog('Received message: {}'.format(filename))
                    #self.briefcam_obj.process_new_file(filename)
        except KeyboardInterrupt:
            logging_to_console_and_syslog("Keyboard interrupt. {}".format(sys.exc_info()[0]))
            raise KeyboardInterrupt
        except:
            logging_to_console_and_syslog(
                "Exception occured while polling for a message from kafka Queue. ".format(sys.exc_info()[0]))

if __name__ == '__main__':
    poll_instance = Poll_for_new_file_name()
    poll_instance.load_environment_variables()
    poll_instance.connect_to_xhost_environment()
    continue_poll = True
    try:
        while continue_poll:
            poll_instance.connect_and_poll_for_new_message()
    except KeyboardInterrupt:
        logging_to_console_and_syslog("Keyboard interrupt." + sys.exc_info()[0])
        continue_poll = False
