#!/usr/bin/env python3
import time

time.sleep(2)

from kafka import KafkaConsumer
import os
import sys, traceback

import logging
from log.log_file import logging_to_console_and_syslog
from redisClient.RedisClient import RedisClient
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
        self.total_job_done_count_redis_name = None
        self.redis_log_keyname = None
        import upload_video.upload_video_to_briefcam
        self.briefcam_obj = None
        self.hostname = os.popen("cat /etc/hostname").read()
        self.cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()
        self.redis_instance = RedisClient()

    def load_environment_variables(self):
        while self.broker_name is None or \
                self.topic is None or \
                self.redis_log_keyname is None or \
                self.total_job_done_count_redis_name is None :
            time.sleep(2)
            self.topic = os.getenv("topic_key",
                                   default=None)
            self.broker_name = os.getenv("broker_name_key",
                                         default=None)
            self.redis_log_keyname = os.getenv("redis_log_keyname_key",
                                         default=None)
            self.total_job_done_count_redis_name = os.getenv("total_job_done_count_redis_name_key",
                                         default=None)

        logging_to_console_and_syslog("redis_log_keyname={}"
                                      .format(self.redis_log_keyname),
                                      logging.INFO)

        logging_to_console_and_syslog("broker_name={}"
                                      .format(self.broker_name),
                                      logging.INFO)
        logging_to_console_and_syslog("topic={}"
                                      .format(self.topic),
                                      logging.INFO)
        logging_to_console_and_syslog("total_job_done_count_redis_name={}"
                                      .format(self.total_job_done_count_redis_name),
                                      logging.INFO)
    def connect_to_kafka_broker(self):
        self.consumer_instance = None
        while self.consumer_instance is None:
            # group_id = "consumer" + self.cont_id[:12]
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
            self.consumer_instance.subscribe([self.topic])
            while continue_inner_poll:
                for msg in self.consumer_instance:
                    filename = msg.value.decode('utf-8')
                    start_time = datetime.now()
                    event = 'Going to process {}'.format(filename)
                    self.redis_instance.write_an_event_on_redis_db(event)
                    self.redis_instance.write_an_event_on_redis_db(event, self.redis_log_keyname)
                    self.briefcam_obj.process_new_file(filename)
                    time_elapsed = datetime.now() - start_time
                    try:
                        event = 'Time taken to process {} = (hh:mm:ss.ms) {}'.format(filename, time_elapsed)
                        self.redis_instance.write_an_event_on_redis_db(event)
                        self.redis_instance.write_an_event_on_redis_db(event,self.redis_log_keyname)
                        self.redis_instance.increment_key_in_redis_db(self.total_job_done_count_redis_name)
                    except:
                        logging_to_console_and_syslog("caught an exception when trying to write to redisClient")
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
                msgs = {}
                msgs = self.consumer_instance.poll(timeout_ms=100, max_records=1)
                for msg in msgs.values():
                    # for msg in self.consumer_instance:
                    # filename = msg.value.decode('utf-8')
                    # logging_to_console_and_syslog('Received message: {}'.format(filename))
                    logging_to_console_and_syslog('Received message: {}'.format(repr(msgs)))
                    logging_to_console_and_syslog('msg: {}'.format(repr(msg)))
                    start_time = datetime.now()
                    # self.briefcam_obj.process_new_file(filename)
                    time_elapsed = datetime.now() - start_time
                    try:
                        # event='Time taken to process {} = (hh:mm:ss.ms) {}'.format(filename, time_elapsed)
                        # self.write_an_event_on_redis_db(event)
                        time.sleep(5)
                    except:
                        logging_to_console_and_syslog("caught an exception when trying to write to redisClient")
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
    continue_poll = True
    try:
        while continue_poll:
            poll_instance.connect_and_poll_for_new_message()
    except KeyboardInterrupt:
        logging_to_console_and_syslog("Keyboard interrupt." + sys.exc_info()[0])
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)

        continue_poll = False
