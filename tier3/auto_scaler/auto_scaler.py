import time
import os
import sys


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
from infrastructure_components.redis_client.redis_interface import RedisInterface
from infrastructure_components.build_ut_push_docker_image.docker_api_interface import DockerAPIInterface


class AutoScaler:
    def __init__(self):
        self.redis_instance = RedisInterface("AutoScaler")
        self.docker_api_interface = DockerAPIInterface()
        self.min_threshold = -1
        self.max_threshold = -1
        self.load_environment_variables()

    def load_environment_variables(self):
        while self.min_threshold is -1 or \
              self.max_threshold is -1:
            time.sleep(1)
            self.min_threshold = os.getenv("min_threshold_key", default=-1)
            self.max_threshold = os.getenv("max_threshold_key", default=-1)
        logging_to_console_and_syslog(("min_threshold={}".format(self.min_threshold)))
        logging_to_console_and_syslog(("max_threshold={}".format(self.max_threshold)))

    def perform_auto_scaling(self):
        while True:
            pass

    def cleanup(self):
        pass
