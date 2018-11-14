import os
import time
import sys
import traceback
import unittest

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
from orchestrator import RTSPRecorderOrchestrator

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
        self.rtsp_orchestrator = RTSPRecorderOrchestrator()

    def yield_container(self):
        for container_id in self.rtsp_orchestrator.yield_container():
            self.assertTrue(self.rtsp_orchestrator.check_if_container_is_active(container_id))
        self.assertFalse(self.rtsp_orchestrator.check_if_container_is_active("123"))

    def test_run_stop_container(self):
        container_id = self.rtsp_orchestrator.run_container("{foo:bar}")
        time.sleep(5)
        self.assertIsNotNone(container_id)
        self.yield_container()
        if container_id:
            self.rtsp_orchestrator.stop_container(container_id)
            time.sleep(5)
            self.assertFalse(self.rtsp_orchestrator.check_if_container_is_active(container_id))
        """
        Delete all containers.            
        for container_id in self.rtsp_orchestrator.yield_container():
            self.rtsp_orchestrator.stop_container(container_id)
        """

    def tearDown(self):
        self.rtsp_orchestrator.cleanup()

if __name__ == "__main__":
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