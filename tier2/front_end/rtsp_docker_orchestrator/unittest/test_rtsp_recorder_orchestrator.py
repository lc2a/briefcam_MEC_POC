import os
import time
import sys
import unittest

#sys.path.append("..")  # Adds higher directory to python modules path.

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

from tier2.front_end.rtsp_docker_orchestrator.orchestrator import RTSPDockerOrchestrator
from infrastructure_components.log.log_file import logging_to_console_and_syslog

#unit tests
class TestOrchestrator(unittest.TestCase):
    def setUp(self):
        self.rtsp_orchestrator = None
        os.environ["image_name_key"] = "ssriram1978/rtsp_recorder:latest"
        os.environ["environment_key"] = "video_file_path_key=/data " \
                                        "rtsp_file_name_prefix_key=briefcam " \
                                        "rtsp_duration_of_the_video_key=30 " \
                                        "min_file_size_key=10000000" \
                                        "rtsp_capture_application_key=openRTSP "
        os.environ["bind_mount_key"] = "/var/run/docker.sock:/var/run/docker.sock /usr/bin/docker:/usr/bin/docker"
        self.rtsp_orchestrator = RTSPDockerOrchestrator()

    def yield_container(self):
        logging_to_console_and_syslog("Unit testing function yield_container()")
        for container_id in self.rtsp_orchestrator.yield_container():
            logging_to_console_and_syslog("Unit testing function check_if_container_is_active()")
            self.assertTrue(self.rtsp_orchestrator.check_if_container_is_active(container_id))
        logging_to_console_and_syslog("Unit testing function check_if_container_is_active()")
        self.assertFalse(self.rtsp_orchestrator.check_if_container_is_active("123"))

    def test_run_stop_container(self):
        container_id = self.rtsp_orchestrator.run_container("{foo:bar}")
        time.sleep(5)
        self.assertIsNotNone(container_id)
        self.yield_container()
        if container_id:
            logging_to_console_and_syslog("Unit testing function stop_container()")
            self.rtsp_orchestrator.stop_container(container_id)
            time.sleep(5)
            logging_to_console_and_syslog("Unit testing function check_if_container_is_active()")
            self.assertFalse(self.rtsp_orchestrator.check_if_container_is_active(container_id))
        """
        Delete all containers.            
        for container_id in self.rtsp_orchestrator.yield_container():
            self.rtsp_orchestrator.stop_container(container_id)
        """

    def tearDown(self):
        self.rtsp_orchestrator.cleanup()


if __name__ == "__main__":
    unittest.main()
