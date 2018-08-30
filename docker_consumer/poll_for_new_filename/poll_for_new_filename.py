#!/usr/bin/env python3

import time
time.sleep(2)

from confluent_kafka import Consumer, KafkaError
import os
import logging
import sys


class poll_for_new_file_name:
    def __init__(self):
        self.broker_name = None
        self.topic = None
        self.consumer_instance = None
        import upload_video.upload_video_to_briefcam
        self.briefcam_obj= None
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
		while self.broker_name == None and self.topic == None:
			self.broker_name = os.getenv("broker_name_key", default=None)
			logging.debug("Trying to read the environment variables, broker_name_key and topic_key")
			self.topic = os.getenv("topic_key", default=None)
        logging.debug("broker_name={}".format(self.broker_name))
		logging.debug("topic={}".format(self.topic))

    def connect_to_kafka_broker(self):
        while self.consumer_instance == None:
            self.consumer_instance = Consumer({
                'bootstrap.servers': self.broker_name,
                'group.id': 'mygroup',
                'default.topic.config': {
                    'auto.offset.reset': 'smallest'
                }
            })
        logging.debug('Successfully attached to bootstrap server={},'.format(self.broker_name))
        self.consumer_instance.subscribe([self.topic])
        logging.debug('Successfully subscribed to topic={},'.format(self.topic))

    def connect_to_xhost_environment(self):
        connected = False
        sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        #sys.path.append('..\\upload_video.upload_video_to_briefcam')
        while connected == False:
            try:
                from upload_video.upload_video_to_briefcam import upload_video_to_briefcam
            except:
                logging.debug("Unable to import module upload_video" + sys.exc_info()[0])
                print("Unable to import module upload_video" + sys.exc_info()[0])
            else:
                logging.debug("successfully connected to xhost display")
                print("successfully connected to xhost display")
                self.briefcam_obj=upload_video_to_briefcam()
                connected = True

    def poll_for_new_message(self):
        continue_poll=True
        while continue_poll == True:
            try:
                msg = self.consumer_instance.poll(1.0)
                if msg is None:
                    logging.debug("No message in Kafka Topic={}".format(self.topic))
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.debug("Error message received from KafkaQ.KafkaError._PARTITION_EOF")
                    else:
                        logging.debug("Kafka error response: " + msg.error())
                    continue
                else:
                    filename = msg.value().decode('utf-8')
                    logging.debug('Received message: {}'.format(filename))
                    self.briefcam_obj.process_new_file(filename)

            except KeyboardInterrupt:
                logging.debug("Keyboard interrupt." + sys.exc_info()[0])
                print("Keyboard interrupt." + sys.exc_info()[0])
                continue_poll=False

            except:
                logging.debug("Exception occured while polling for a message from kafka Queue." + sys.exc_info()[0])
                print("Exception occured while polling for a message from kafka Queue." + sys.exc_info()[0])
        self.consumer_instance.close()


if __name__=='__main__':
    poll_instance = poll_for_new_file_name()
    poll_instance.load_environment_variables()
    poll_instance.connect_to_kafka_broker()
    poll_instance.connect_to_xhost_environment()
    poll_instance.poll_for_new_message()
