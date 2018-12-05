import os
import time
import sys
import subprocess
import unittest
import traceback

def import_all_packages():
    realpath = os.path.realpath(__file__)
    # print("os.path.realpath({})={}".format(__file__, realpath))
    dirname = os.path.dirname(realpath)
    # print("os.path.dirname({})={}".format(realpath, dirname))
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
from infrastructure_components.data_parser.data_parser_interface import DataParserInterface


class TestDataParser(unittest.TestCase):
    def setUp(self):
        self.dirname = os.path.dirname(os.path.realpath(__file__))
        self.filename = 'camera1_2018_11_15_14_42_55_745282.mp4'
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["total_job_enqueued_count_redis_name_key"] = "enqueue"
        os.environ["total_job_dequeued_count_redis_name_key"] = "dequeue"
        os.environ["redis_server_hostname_key"] = "10.1.100.100"
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["redis_server_port_key"] = "6379"
        self.data_parser_instance = None

    def test_run(self):
        """
        logging_to_console_and_syslog("Testing pytorch parser.")
        self.create_test_delete_test(DataParserInterface.PyTorch,
                                'docker-compose_pytorch.yml')
        logging_to_console_and_syslog("Testing tensorflow parser.")
        self.create_test_delete_test(DataParserInterface.TensorFlow,
                                'docker-compose_tensorflow.yml')
        """
        logging_to_console_and_syslog("Testing briefcam parser.")
        self.create_test_delete_test(DataParserInterface.BriefCam,
                                     'docker-compose_briefcam.yml')

    def create_test_delete_test(self, parser_type, docker_compose_yml_file):
        self.create_docker_container(docker_compose_yml_file)
        time.sleep(10)
        os.environ["data_parser_type_key"] = parser_type

        if parser_type is DataParserInterface.BriefCam:
            os.environ["case_name_key"] = "MEC-POC"
            os.environ["case_url_key"] = "http://mec-demo/synopsis/"
            os.environ["browser_name_key"] = "/opt/google/chrome/chrome"
            os.environ["browser_loc_key"] = "google-chrome"
            os.environ["login_username_key"] = "Brief"
            os.environ["login_password_key"] = "Cam"
            os.environ["image_directory"] = "image_cont"
            os.environ["max_retry_attempts_key"] = "8"
            os.environ["sleep_time_key"] = "1"
            os.environ["time_between_input_character_key"] = "0.1"
            os.environ["time_for_browser_to_open_key"] = "60"
            os.environ["total_job_done_count_redis_name_key"] = "total_job_done_count"
            os.environ["video_file_path_key"] = self.dirname

        self.data_parser_instance = DataParserInterface()
        logging_to_console_and_syslog("Validating Data Parser Instance to be not null.")
        self.assertIsNotNone(self.data_parser_instance)
        self.assertTrue(self.data_parser_instance.process_job(self.filename))
        self.delete_docker_container(docker_compose_yml_file)

    def create_docker_container(self,docker_compose_yml_file):
        logging_to_console_and_syslog("Invoking docker-compose up for {} from directory {}"
                                      .format(docker_compose_yml_file,
                                              self.dirname))

        completedProcess = subprocess.run(["sudo",
                                           "docker-compose",
                                           "-f",
                                           "{}/{}".format(self.dirname,
                                                          docker_compose_yml_file),
                                           "up",
                                           "-d"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        logging_to_console_and_syslog(completedProcess.stdout.decode('utf8'))
        time.sleep(15)

    def delete_docker_container(self, docker_compose_yml_file):
        logging_to_console_and_syslog("Invoking docker-compose down for {} from directory {}."
                                      .format(docker_compose_yml_file,
                                              self.dirname))

        completedProcess = subprocess.run(["sudo",
                                           "docker-compose",
                                           "-f",
                                           "{}/{}".format(self.dirname,
                                                          docker_compose_yml_file),
                                           "down"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        logging_to_console_and_syslog(completedProcess.stdout.decode('utf8'))
        time.sleep(15)

    def tearDown(self):
        self.data_parser_instance.cleanup()


if __name__ == "__main__":
    try:
        unittest.main()
    except:
        logging_to_console_and_syslog("Exception occurred.{}".format(sys.exc_info()))
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
