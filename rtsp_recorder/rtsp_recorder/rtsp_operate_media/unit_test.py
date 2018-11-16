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

    def create_subprocess(self,process_args):
        if not process_args or type(process_args) != list:
            return None

        completedProcess = subprocess.run(process_args,
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)
        return completedProcess.stdout

    def create_rtsp_operate_media_docker_image(self):
        docker_create_command_list = ["docker",
                                      "build",
                                         ".",
                                      "-t",
                                      "ssriram1978/rtsp_operate_media_ut:latest"]
        self.assertIsNotNone(self.create_subprocess(docker_create_command_list))

    def run_rtsp_operate_media_docker_container(self):
        docker_run_command_list = ["docker",
                                    "run",
                                    "-it",
                                    "--name",
                                    "rtsp_operate_media_ut",
                                    "-d",
                                    "ssriram1978/rtsp_operate_media_ut:latest"]
        self.assertIsNotNone(self.create_subprocess(docker_run_command_list))

    def wait_for_rtsp_operate_media_docker_container(self):
        docker_wait_command_list = ["docker",
                                    "wait",
                                    "rtsp_operate_media_ut"]
        self.assertEqual(self.create_subprocess(docker_wait_command_list), b'0\n')

    def prune_old_rtsp_operate_media_docker_image(self):
        docker_prune_command_list = ["docker",
                                    "container",
                                    "prune",
                                    "-f"]
        self.assertIsNotNone(self.create_subprocess(docker_prune_command_list))

    def capture_rtsp_operate_media_docker_container_logs(self):
        docker_container_log_list = ["docker",
                                     "logs",
                                     "rtsp_operate_media_ut"]
        logging_to_console_and_syslog(self.create_subprocess(docker_container_log_list).decode())

    def remove_rtsp_operate_media_docker_image(self):
        docker_container_remove_image_list = ["docker",
                                     "image",
                                     "rm",
                                     "-f",
                                     "ssriram1978/rtsp_operate_media_ut"]
        self.assertIsNotNone(self.create_subprocess(docker_container_remove_image_list))

    def test_rtsp_start_stop_media(self):
        logging_to_console_and_syslog("Creating Docker image.")
        self.create_rtsp_operate_media_docker_image()
        logging_to_console_and_syslog("Prune old docker container images.")
        self.prune_old_rtsp_operate_media_docker_image()
        logging_to_console_and_syslog("Running Docker image.")
        self.run_rtsp_operate_media_docker_container()
        logging_to_console_and_syslog("Waiting for the Docker image to complete.")
        self.wait_for_rtsp_operate_media_docker_container()
        logging_to_console_and_syslog("Capturing container logs.")
        self.capture_rtsp_operate_media_docker_container_logs()
        logging_to_console_and_syslog("Removing docker image.")
        #self.remove_rtsp_operate_media_docker_image()
        logging_to_console_and_syslog("Completed unit testing of rtsp_operate_media.")

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
