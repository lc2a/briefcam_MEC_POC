import os
import sys
import time
import unittest
import traceback
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
from infrastructure_components.log.log_file import logging_to_console_and_syslog
from infrastructure_components.open_rtsp_api_handler.open_rtsp_api_handler import OpenRTSPAPIHandler


class TestRTSPOperateMedia(unittest.TestCase):
    def setUp(self):
        os.environ["video_file_path_key"] = "/data"
        os.environ["rtsp_file_name_prefix_key"] = "briefcam"
        os.environ["rtsp_capture_application_key"] = "openRTSP"
        os.environ["rtsp_duration_of_the_video_key"] = "30"
        os.environ["rtsp_message_key"] = "{'name': 'camera1', 'ip': '10.136.66.233'}"
        os.environ["min_file_size_key"] = "10000000"
        self.rtsp_media_instance = OpenRTSPAPIHandler()

    def test_rtsp_start_stop_media(self):
        logging_to_console_and_syslog("Unit testing function start_rtsp_stream()")
        self.assertNotEqual(self.rtsp_media_instance.start_rtsp_stream(),0)
        logging_to_console_and_syslog("Unit testing function check_rtsp_stream()")
        self.assertTrue(self.rtsp_media_instance.check_rtsp_stream())
        logging_to_console_and_syslog("Unit testing function stop_rtsp_stream()")
        self.rtsp_media_instance.stop_rtsp_stream()
        logging_to_console_and_syslog("Unit testing function check_rtsp_stream()")
        self.assertFalse(self.rtsp_media_instance.check_rtsp_stream())
        logging_to_console_and_syslog("Unit testing function start_rtsp_stream()")
        self.assertNotEqual(self.rtsp_media_instance.start_rtsp_stream(),0)
        logging_to_console_and_syslog("Unit testing function move_media_files_to_shared_directory()")
        while self.rtsp_media_instance.move_media_files_to_shared_directory():
            time.sleep(1)
            logging_to_console_and_syslog("Unit testing function check_rtsp_stream()")
            if not self.rtsp_media_instance.check_rtsp_stream():
                break

    def tearDown(self):
        if self.rtsp_media_instance:
            self.rtsp_media_instance.cleanup()
            self.rtsp_media_instance.stop_rtsp_stream()


if __name__ == "__main__":
    unittest.main()