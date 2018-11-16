import os
import sys
import time
import unittest
import traceback
from rtsp_operate_media import RtspOperationsOnMedia
sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog

class TestRTSPOperateMedia(unittest.TestCase):
    def setUp(self):
        os.environ["video_file_path_key"] = "/data"
        os.environ["rtsp_file_name_prefix_key"] = "briefcam"
        os.environ["rtsp_capture_application_key"] = "openRTSP"
        os.environ["rtsp_duration_of_the_video_key"] = "30"
        os.environ["rtsp_message_key"] = "{'name': 'camera1', 'ip': '10.136.66.233'}"
        os.environ["min_file_size_key"] = "10000000"
        self.rtsp_media_instance = RtspOperationsOnMedia()

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