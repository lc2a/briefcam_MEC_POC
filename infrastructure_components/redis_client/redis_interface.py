import os
import sys, traceback
import time

def import_all_packages():
    realpath=os.path.realpath(__file__)
    #print("os.path.realpath({})={}".format(__file__,realpath))
    dirname=os.path.dirname(realpath)
    #print("os.path.dirname({})={}".format(realpath,dirname))
    dirname_list=dirname.split('/')
    #print(dirname_list)
    for index in range(len(dirname_list)):
        module_path='/'.join(dirname_list[:index])
        #print("module_path={}".format(module_path))
        try:
            sys.path.append(module_path)
        except:
            #print("Invalid module path {}".format(module_path))
            pass

import_all_packages()

from infrastructure_components.redis_client.redis_client import RedisClient
from infrastructure_components.log.log_file import logging_to_console_and_syslog

class RedisInterface:
    """
    This class does the following:
    """

    def __init__(self,thread_identifer=None):
        logging_to_console_and_syslog("Instantiating RedisInterface.")
        self.total_job_enqueued_count_redis_name = None
        self.total_job_dequeued_count_redis_name = None
        self.redis_log_keyname = None
        self.thread_identifer = thread_identifer
        self.read_environment_variables()
        self.redis_instance = RedisClient()

    def read_environment_variables(self):
        """
        This method is used to read the environment variables
        defined in the OS.
        :return:
        """
        while self.redis_log_keyname is None or \
                self.total_job_dequeued_count_redis_name is None:
            time.sleep(2)
            logging_to_console_and_syslog("RedisInterface:{} "
                                          "Trying to read the "
                                          "environment variables..."
                                          .format(self.thread_identifer))
            self.redis_log_keyname = os.getenv("redis_log_keyname_key",
                                               default=None)
            self.total_job_enqueued_count_redis_name = os.getenv("total_job_enqueued_count_redis_name_key",
                                                                 default=None)
            self.total_job_dequeued_count_redis_name = os.getenv("total_job_dequeued_count_redis_name_key",
                                                                 default=None)
        logging_to_console_and_syslog("RedisInterface:{} "
                                      "redis_log_keyname={}, "
                                      "total_job_enqueued_count_redis_name={}, "
                                      "total_job_dequeued_count_redis_name={}. "
                                      .format(self.thread_identifer,
                                              self.redis_log_keyname,
                                              self.total_job_enqueued_count_redis_name,
                                              self.total_job_dequeued_count_redis_name))

    def get_current_enqueue_count(self):
        logging_to_console_and_syslog("RedisInterface:{}."
                                      "total_job_enqueued_count={}"
                                      .format(self.thread_identifer,
                                              self.total_job_enqueued_count_redis_name))
        return self.redis_instance.read_key_value_from_redis_db(self.total_job_enqueued_count_redis_name)

    def get_current_dequeue_count(self):
        logging_to_console_and_syslog("RedisInterface:{}."
                                      "total_job_dequeued_count={}"
                                      .format(self.thread_identifer,
                                              self.total_job_dequeued_count_redis_name))
        return self.redis_instance.read_key_value_from_redis_db(self.total_job_dequeued_count_redis_name)

    def increment_enqueue_count(self):
        logging_to_console_and_syslog("RedisInterface:{}."
                                      "Incrementing total_job_enqueued_count={}"
                                      .format(self.thread_identifer,
                                              self.total_job_enqueued_count_redis_name))
        self.redis_instance.increment_key_in_redis_db(self.total_job_enqueued_count_redis_name)

    def increment_dequeue_count(self):
        logging_to_console_and_syslog("RedisInterface:{}."
                                      "Incrementing total_job_dequeued_count={}"
                                      .format(self.thread_identifer,
                                              self.total_job_dequeued_count_redis_name))
        self.redis_instance.increment_key_in_redis_db(self.total_job_dequeued_count_redis_name)

    def check_if_the_key_exists_in_redis_db(self, key):
        logging_to_console_and_syslog("RedisInterface:{}."
                                      "check_if_the_key_exists_in_redis_db key={}"
                                      .format(self.thread_identifer,
                                              key))
        return self.redis_instance.check_if_the_key_exists_in_redis_db(key)

    def set_the_key_in_redis_db(self, key):
        logging_to_console_and_syslog("RedisInterface:{}."
                                      "set_the_key_in_redis_db key={}"
                                      .format(self.thread_identifer,
                                              key))
        return self.redis_instance.set_the_key_in_redis_db(key)

    def write_an_event_in_redis_db(self, event):
        logging_to_console_and_syslog("RedisInterface:{}."
                                      "Writing at key={}"
                                      "event={}"
                                      .format(self.thread_identifer,
                                              self.redis_log_keyname,
                                              event))
        self.redis_instance.write_an_event_on_redis_db(event, self.redis_log_keyname)

    def cleanup(self):
        pass
