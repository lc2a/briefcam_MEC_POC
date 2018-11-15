import os
import sys
import time
import unittest
import traceback
import subprocess

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog


class TestRTSPOperateMediaDocker(unittest.TestCase):
    def setUp(self):
        pass

    def create_rtsp_operate_media_docker_containers(self):
        completedProcess = subprocess.run(["docker",
                                           "run",
                                           "-it",
                                           "--name",
                                           "rtsp_operate_media_ut",
                                           "-d",
                                           "ssriram1978/rtsp_operate_media_ut:latest"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def wait_for_rtsp_operate_media_docker_containers(self):
        completedProcess = subprocess.run(["docker",
                                           "wait",
                                           "rtsp_operate_media_ut"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)
        print("completedProcess.stdout = {}".format(completedProcess.stdout))
        self.assertNotEqual(completedProcess.stdout,0)

    def test_rtsp_start_stop_media(self):
        self.create_rtsp_operate_media_docker_containers()
        self.wait_for_rtsp_operate_media_docker_containers()

    def tearDown(self):
        pass


if __name__ == "__main__":
    # debugging code.
    try:
        unittest.main()
    except KeyboardInterrupt:
        logging_to_console_and_syslog("You terminated the program by pressing ctrl + c")
    except BaseException:
        logging_to_console_and_syslog("Base Exception occurred {}.".format(sys.exc_info()[0]))
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
        time.sleep(5)
    except:
        logging_to_console_and_syslog("Unhandled exception {}.".format(sys.exc_info()[0]))
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
        time.sleep(5)
    finally:
        pass
