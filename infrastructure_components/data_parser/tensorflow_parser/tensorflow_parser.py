#!/usr/bin/env python3
import os
import sys


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
from infrastructure_components.redis_client.redis_interface import RedisInterface


class TensorFlowParser:

    redis_instance = None

    def __init__(self):
        logging_to_console_and_syslog("**********Initializing Tensor Flow Parser ***********")
        self.hostname = os.popen("cat /etc/hostname").read()
        self.cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()
        TensorFlowParser.redis_instance = RedisInterface("BriefCam+{}".format(self.cont_id))

    def process_job(self, message):
        logging_to_console_and_syslog("**********Initializing Tensor Flow processing_job {} ***********"
                                      .format(message))
        return True

    def clean_up(self):
        pass
