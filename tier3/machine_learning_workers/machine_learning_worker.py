#!/usr/bin/env python3
import time
import os
import sys, traceback
from datetime import datetime


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
from infrastructure_components.producer_consumer.producer_consumer import ProducerConsumerAPI
from infrastructure_components.redisClient.redis_interface import RedisInterface
from infrastructure_components.data_parser.data_parser_interface import DataParserInterface


class MachineLearningWorker:
    def __init__(self):
        self.hostname = os.popen("cat /etc/hostname").read()
        self.cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()
        self.producer_consumer_queue_type = None
        self.load_environment_variables()
        self.consumer_instance = None
        self.data_parser_instance = None
        self.redis_instance = None
        self.instantiate_objects()

    def load_environment_variables(self):
        while self.producer_consumer_queue_type is None:
            time.sleep(1)
            self.producer_consumer_queue_type = os.getenv("producer_consumer_queue_type_key",
                                                          default=None)

        logging_to_console_and_syslog(("producer_consumer_queue_type={}"
                                       .format(self.producer_consumer_queue_type)))

    def instantiate_objects(self):
        self.consumer_instance = ProducerConsumerAPI(is_consumer=True,
                                                     thread_identifier="Consumer_{}".format(self.cont_id),
                                                     type_of_messaging_queue=self.producer_consumer_queue_type)
        self.data_parser_instance = DataParserInterface()
        self.redis_instance = RedisInterface("Consumer_{}".format(self.cont_id))

    def cleanup(self):
        self.consumer_instance.cleanup()

    def process_job(self, message):
        self.data_parser_instance.process_job(message)

    def dequeue_and_process_jobs(self):
        for message in self.consumer_instance.dequeue():
            event = "Consumer: Successfully dequeued a message = {} from msgQ.".format(message)
            self.redis_instance.write_an_event_in_redis_db(event)
            self.redis_instance.increment_dequeue_count()
            start_time = datetime.now()
            self.process_job(message)
            time_elapsed = datetime.now() - start_time
            event = 'Time taken to process {} = (hh:mm:ss.ms) {}'.format(message, time_elapsed)
            self.redis_instance.write_an_event_in_redis_db(event)


if __name__ == '__main__':
    worker = MachineLearningWorker()
    continue_poll = True
    try:
        while continue_poll:
            worker.dequeue_and_process_jobs()
    except KeyboardInterrupt:
        logging_to_console_and_syslog("Keyboard interrupt." + sys.exc_info()[0])
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)

        continue_poll = False
