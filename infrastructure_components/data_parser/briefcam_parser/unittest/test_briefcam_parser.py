#!/usr/bin/env python3
import time
import os
import sys
import unittest

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


class TestBriefCamParser(unittest.TestCase):
    def setUp(self):
        self.dirname = os.path.dirname(os.path.realpath(__file__))
        self.briefcam_parser_instance = None
        self.filename = 'camera1_2018_11_15_14_42_55_745282.mp4'
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["total_job_enqueued_count_redis_name_key"] = "enqueue"
        os.environ["total_job_dequeued_count_redis_name_key"] = "dequeue"
        os.environ["redis_server_hostname_key"] = "localhost"
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["redis_server_port_key"] = "6379"
    def test_run(self):
        logging_to_console_and_syslog("Testing Briefcam parser.")
        #self.create_test_delete_test(DataParserInterface.BriefCam,
        #                             'docker-compose_briefcam.yml')

    def tearDown(self):
        #self.briefcam_parser_instance.cleanup()
        pass
