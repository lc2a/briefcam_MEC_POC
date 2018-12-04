import os
import sys
import traceback
import unittest

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
from infrastructure_components.build_ut_push_docker_image.docker_api_interface import DockerAPIInterface


class DockerBuildUTPublish(DockerAPIInterface, unittest.TestCase):

    def __init__(self,
                 docker_tag=None,
                 image_name=None,
                 dockerfile_path=None):
        if not dockerfile_path:
            logging_to_console_and_syslog("You need to specify the path to Dockerfile.")
            raise BaseException

        if image_name:
            self.image_name = image_name
        else:
            self.image_name = dockerfile_path.split('/')[-1]

        self.dirname = dockerfile_path
        self.docker_tag = docker_tag
        logging_to_console_and_syslog("docker_tag={},"
                                      "image_name={},"
                                      "dirname={}"
                                      .format(docker_tag,
                                              self.image_name,
                                              self.dirname))
        super().__init__(docker_tag,
                         self.image_name,
                         self.dirname)
        self.docker_image = None

    def create_docker_image(self):
        logging_to_console_and_syslog("Creating DockerAPIInterface instance()")
        self.docker_image = DockerAPIInterface(docker_tag=self.docker_tag,
                                               image_name=self.image_name,
                                               dockerfile_directory_name=self.dirname)

        logging_to_console_and_syslog("Invoking create_docker_container")

        self.docker_image.create_docker_container()

    def unit_test_this_image(self, ut_file_path):
        logging_to_console_and_syslog("Invoking run_docker_container() unit test.")
        try:
            self.docker_image.run_docker_container('python3 {}'.format(ut_file_path))
        except:
            logging_to_console_and_syslog("Unhandled exception {}.".format(sys.exc_info()[0]))
            print("Exception in user code:")
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)

    def stop_running_this_container(self):
        logging_to_console_and_syslog("Invoking stop_docker_container()")
        self.docker_image.stop_docker_container()

    def remove_docker_image(self):
        logging_to_console_and_syslog("Invoking remove_docker_image()")
        self.docker_image.remove_docker_image()
