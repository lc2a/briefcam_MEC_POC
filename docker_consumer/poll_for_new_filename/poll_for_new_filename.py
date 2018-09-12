#!/usr/bin/env python3
import time

time.sleep(2)

from kafka import KafkaConsumer

import os

import sys, traceback

import redis

import logging

from log.log_file import logging_to_console_and_syslog

from datetime import datetime

def on_assign_partition_to_subscriber(consumer, partitions):
    logging_to_console_and_syslog("partition {} is "
                                  "assigned to the consumer {}"
                                  .format(str(partitions), str(consumer)),
                                  logging.INFO)


class PollForNewFileName:
    def __init__(self):
        self.broker_name = None
        self.topic = None
        self.consumer_instance = None
        import upload_video.upload_video_to_briefcam
        self.briefcam_obj = None
        self.redis_instance=None
        self.redis_server_hostname=None
        self.redis_server_port=0
        self.redis_log_keyname=None
        self.hostname = os.popen("cat /etc/hostname").read()
        self.cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()

    def load_environment_variables(self):
        while self.broker_name is None and \
                self.topic is None and \
                self.redis_server_hostname is None and \
                self.redis_log_keyname is None and \
                self.redis_server_port == 0:
            time.sleep(2)
            logging_to_console_and_syslog("Trying to read the "
                                          "environment variables...")
            self.topic = os.getenv("topic_key",
                                   default=None)
            self.broker_name = os.getenv("broker_name_key",
                                         default=None)
            self.redis_server_hostname = os.getenv("redis_server_hostname_key",
                                                   default=None)
            self.redis_log_keyname = os.getenv("redis_log_keyname_key",
                                               default=None)
            self.redis_server_port = int(os.getenv("redis_server_port_key",
                                                   default=0))

        logging_to_console_and_syslog("broker_name={}"
                                      .format(self.broker_name),
                                      logging.INFO)
        logging_to_console_and_syslog("topic={}"
                                      .format(self.topic),
                                      logging.INFO)
        logging_to_console_and_syslog("redis_server_hostname={}"
                                      .format(self.redis_server_hostname),
                                      logging.INFO)
        logging_to_console_and_syslog("redis_log_keyname={}"
                                      .format(self.redis_log_keyname),
                                      logging.INFO)
        logging_to_console_and_syslog("redis_server_port={}"
                                      .format(self.redis_server_port),
                                      logging.INFO)

    def connect_to_kafka_broker(self):
        self.consumer_instance = None
        while self.consumer_instance is None:
            #group_id = "consumer" + self.cont_id[:12]
            self.consumer_instance = KafkaConsumer(bootstrap_servers=self.broker_name,
                                                   group_id="kafka-consumer")
        logging_to_console_and_syslog('Successfully '
                                      'attached to bootstrap server={},'
                                      .format(self.broker_name),
                                      logging.INFO)

    def connect_to_redis_server(self):
        self.redis_instance=None
        while self.redis_instance == None:
            self.redis_instance=redis.StrictRedis(host=self.redis_server_hostname,
                                              port=self.redis_server_port,
                                              db=0)
        logging_to_console_and_syslog("Successfully connected "
                                      "to redis server {},port {}"
                                      .format(self.redis_server_hostname,
                                              self.redis_server_port),
                                      logging.INFO)

    def write_an_event_on_redis_db(self,event):
        if self.redis_instance is not None:
            event_string = "Hostname=" + self.hostname + " containerID=" + self.cont_id[:12] + event
            logging_to_console_and_syslog("Writing {} to {}".format(event_string, self.redis_log_keyname))
            #ret = self.redis_instance.append(self.redis_log_keyname, event_string)
            #logging_to_console_and_syslog(ret)
    def close_kafka_instance(self):
        if self.consumer_instance:
            self.consumer_instance.close()
            self.consumer_instance=None

    def connect_to_xhost_environment(self):
        connected = False
        sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        # sys.path.append('..\\upload_video.upload_video_to_briefcam')
        while connected == False:
            try:
                from upload_video.upload_video_to_briefcam import UploadVideoToBriefCam
            except:
                logging_to_console_and_syslog("Unable to import "
                                              "module upload_video"
                                              + sys.exc_info()[0],
                                      logging.INFO)
            else:
                logging_to_console_and_syslog("successfully connected "
                                              "to xhost display",
                                      logging.INFO)
                self.briefcam_obj = UploadVideoToBriefCam()
                connected = True

    def connect_and_poll_for_new_message(self):
        self.connect_to_kafka_broker()
        try:
            continue_inner_poll = True
            while continue_inner_poll:
                self.consumer_instance.subscribe([self.topic])
                for msg in self.consumer_instance:
                    filename = msg.value.decode('utf-8')
                    start_time = datetime.now()
                    self.briefcam_obj.process_new_file(filename)
                    time_elapsed = datetime.now() - start_time
                    try:
                        event = 'Time taken to process {} = (hh:mm:ss.ms) {}'.format(filename, time_elapsed)
                        self.write_an_event_on_redis_db(event)
                    except:
                        logging_to_console_and_syslog("caught an exception when trying to write to redis")
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
            self.close_kafka_instance()

    def connect_to_kafka_broker2(self):
        self.consumer_instance = None
        while self.consumer_instance is None:
            self.consumer_instance = KafkaConsumer(self.topic,
                                                   bootstrap_servers=self.broker_name,
                                                   group_id="briefcam-consumer")
        logging_to_console_and_syslog('Successfully attached to '
                                      'bootstrap server={}'
                                      .format(self.broker_name))

    def connect_and_poll_for_new_message2(self):
        try:
            continue_inner_poll = True
            self.connect_to_kafka_broker2()
            while continue_inner_poll:
                msgs={}
                msgs=self.consumer_instance.poll(timeout_ms=100, max_records=1)
                for msg in msgs.values():
                #for msg in self.consumer_instance:
                    #filename = msg.value.decode('utf-8')
                    #logging_to_console_and_syslog('Received message: {}'.format(filename))
                    logging_to_console_and_syslog('Received message: {}'.format(repr(msgs)))
                    logging_to_console_and_syslog('msg: {}'.format(repr(msg)))
                    start_time = datetime.now()
                    #self.briefcam_obj.process_new_file(filename)
                    time_elapsed = datetime.now() - start_time
                    try:
                        #event='Time taken to process {} = (hh:mm:ss.ms) {}'.format(filename, time_elapsed)
                        #self.write_an_event_on_redis_db(event)
                        time.sleep(5)
                    except:
                        logging_to_console_and_syslog("caught an exception when trying to write to redis")
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

            logging_to_console_and_syslog(repr(traceback.format_stack()))
        finally:
            self.close_kafka_instance()

if __name__ == '__main__':
    poll_instance = PollForNewFileName()
    poll_instance.load_environment_variables()
    poll_instance.connect_to_xhost_environment()
    poll_instance.connect_to_redis_server()
    continue_poll = True
    try:
        while continue_poll:
            poll_instance.connect_and_poll_for_new_message()
    except KeyboardInterrupt:
        logging_to_console_and_syslog("Keyboard interrupt."+ sys.exc_info()[0])
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)

        continue_poll = False