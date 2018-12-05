import os
import time
import sys
import redis
import logging
import traceback
import unittest
from datetime import datetime

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

from infrastructure_components.log.log_file import logging_to_console_and_syslog

class RedisClient(object):
    __instance = None

    def __new__(cls):
        if RedisClient.__instance is None:
            RedisClient.__instance = object.__new__(cls)
        return RedisClient.__instance

    def __init__(self):
        logging_to_console_and_syslog("Instantiating RedisClient.")
        self.redis_instance = None
        self.redis_server_hostname = None
        self.redis_server_port = 0
        self.load_environment_variables()
        self.connect_to_redis_server()
        self.hostname = os.popen("cat /etc/hostname").read()
        self.cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()
        self.cont_id = self.cont_id[:12]

    def load_environment_variables(self):
        while self.redis_server_hostname is None or \
                self.redis_server_port == 0:
            time.sleep(2)
            logging_to_console_and_syslog("Redis Client: Trying to read "
            "the environment variables...")
            self.redis_server_hostname = os.getenv("redis_server_hostname_key",
                                                   default=None)
            self.redis_server_port = int(os.getenv("redis_server_port_key",
                                                   default=0))

        logging_to_console_and_syslog("redis_server_hostname={}"
                                      .format(self.redis_server_hostname),
                                      logging.INFO)
        logging_to_console_and_syslog("redis_server_port={}"
                                      .format(self.redis_server_port),
                                      logging.INFO)

    def connect_to_redis_server(self):
        while self.redis_instance is None:
            self.redis_instance = redis.StrictRedis(host=self.redis_server_hostname,
                                                    port=self.redis_server_port,
                                                    db=0)
        logging_to_console_and_syslog("Successfully connected "
                                      "to redisClient server {},port {}"
                                      .format(self.redis_server_hostname,
                                              self.redis_server_port),
                                      logging.INFO)

    def write_an_event_on_redis_db(self, event,key=None):
        return_value = False
        if self.redis_instance is not None:
            current_time = datetime.now()
            event_string = "\n Time={},Hostname={},containerID={},event={}"\
                .format(str(current_time),
                        self.hostname,
                        self.cont_id[:12],
                        event)
            key_name = None
            if key:
                key_name = key
            else:
                key_name = self.cont_id
            if self.redis_instance.exists(key_name):
                self.redis_instance.append(key_name, event_string)
                logging_to_console_and_syslog("Appending "
                "{} to {}".format(event_string, key_name))
                return_value = True
            else:
                self.redis_instance.set(key_name, event_string)
                logging_to_console_and_syslog("Writing "
                "{} to {}".format(event_string, key_name))
                return_value = True
        return return_value

    def check_if_the_key_exists_in_redis_db(self, key):
        return_value = False
        if self.redis_instance is not None:
            if self.redis_instance.exists(key):
                return_value = True
        return return_value

    def set_the_key_in_redis_db(self, key):
        return_value = False
        if self.redis_instance is not None:
            self.redis_instance.set(key, 1)
            return_value = True
        return return_value

    def delete_key_from_redis_db(self, key):
        return_value = False
        if self.redis_instance is not None:
            if self.redis_instance.exists(key):
                if self.redis_instance.delete(key):
                    return_value = True
        return return_value

    def increment_key_in_redis_db(self,key):
        return_value = False
        if self.redis_instance is not None:
            self.redis_instance.incr(key)
            return_value = True
        return return_value

    def read_key_value_from_redis_db(self,key):
        return_value = -1
        if self.redis_instance is not None:
            if self.redis_instance.exists(key):
                return_value = self.redis_instance.get(key)
        return return_value

    def cleanup(self):
        pass
