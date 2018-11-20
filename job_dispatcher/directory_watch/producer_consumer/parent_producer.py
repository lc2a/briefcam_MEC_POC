from kafka import KafkaProducer
import os
import sys, traceback

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
import time
from redisClient.RedisClient import RedisClient


class ParentProducer:
    """
    This class does the following:
    """

    def __init__(self):
        self.total_job_done_count_redis_name = None
        self.redis_log_keyname = None
        self.read_environment_variables()
        self.redis_instance = RedisClient()

    def read_environment_variables(self):
        """
        This method is used to read the environment variables defined in the OS.
        :return:
        """
        while self.redis_log_keyname is None or \
                self.total_job_done_count_redis_name is None:
            time.sleep(2)
            logging_to_console_and_syslog("ParentProducer: Trying to read the environment variables...")
            self.redis_log_keyname = os.getenv("redis_log_keyname_key",
                                               default=None)
            self.total_job_done_count_redis_name = os.getenv("total_produced_count_redis_name_key",
                                                             default=None)
        logging_to_console_and_syslog("ParentProducer: redis_log_keyname={}".format(self.redis_log_keyname))
        logging_to_console_and_syslog("ParentProducer: total_produced_count_redis_name={}".format(self.total_job_done_count_redis_name))

    def get_current_job_count(self):
        return self.redis_instance.read_key_value_from_redis_db(self.total_job_done_count_redis_name)

    def increment_job_produced_count(self):
        self.redis_instance.increment_key_in_redis_db(self.total_job_done_count_redis_name)

    def write_an_event_in_redis_db(self, event):
        self.redis_instance.write_an_event_on_redis_db(event, self.redis_log_keyname)

    def cleanup(self):
        pass
