#!/usr/bin/env python3
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


class DataParserInterface:
    BriefCam = "BriefCam"
    TensorFlow = "TensorFlow"
    PyTorch = "PyTorch"

    def __init__(self):
        self.data_parser_type = None
        self.parser_instance = None
        self.hostname = os.popen("cat /etc/hostname").read()
        self.cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()
        self.load_environment_variables()
        self.__instantiate_objects()

    def load_environment_variables(self):
        while self.data_parser_type is None:
            time.sleep(1)
            self.data_parser_type = os.getenv("data_parser_type_key", default=None)
        logging_to_console_and_syslog(("DataParserInterface: data_parser_type={}"
                                       .format(self.data_parser_type)))

    def __instantiate_objects(self):
        logging_to_console_and_syslog("DataParserInterface: instantiate_objects()")
        if self.data_parser_type == DataParserInterface.BriefCam:
            from infrastructure_components.data_parser.briefcam_parser.briefcam_parser import BriefCamParser
            logging_to_console_and_syslog("Instantiating BriefCamParser instance.")
            self.parser_instance = BriefCamParser()
        elif self.data_parser_type == DataParserInterface.TensorFlow:
            from infrastructure_components.data_parser.tensorflow_parser.tensorflow_parser import TensorFlowParser
            logging_to_console_and_syslog("Instantiating TensorFlowParser instance.")
            self.parser_instance = TensorFlowParser()
        elif self.data_parser_type == DataParserInterface.PyTorch:
            from infrastructure_components.data_parser.pytorch_parser.pytorch_parser import PyTorchParser
            logging_to_console_and_syslog("Instantiating PyTorchParser instance.")
            self.parser_instance = PyTorchParser()

    def cleanup(self):
        self.parser_instance.clean_up()

    def process_job(self, filename):
        if not self.parser_instance:
            logging_to_console_and_syslog("parser instance is None.")
            return False
        return self.parser_instance.process_job(filename)