#!/usr/bin/env python3
import time
import os
import sys
import traceback
from sys import path


#path.append(os.getcwd())

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


class RtspRecorder:
    """
    This class does the following
    1. It instantiates OpenRTSPAPIHandler class object.
    2. It controls OpenRTSPAPIHandler class object by starting and stoping RTSP media operation.
    3. It checks if the RTSP video capturing is going on and if it finds that the process is dead or in
    defunct state, it kills the process and restarts it.
    4. It checks for the video capture media files to be stored in the shared mount, and if it finds that
    there are no media files being produced by OpenRTSPAPIHandler class, then, it stops the operation
    and restarts it.
    """
    def __init__(self):
        """
        Define all handles and init variables.
        """
        self.rtsp_media_instance = None
        self.initialize_instances()
        self.rtsp_message = None
        self.skipped_media_file_creation = 0
        self.max_media_file_skipped_count = 5

    def initialize_instances(self):
        """
        Initialize the rtsp media instance variable which instantiates OpenRTSPAPIHandler class object.
        """
        self.rtsp_media_instance = OpenRTSPAPIHandler()

    def check_and_restart_rtsp_video_capture(self):
        """
        If the RTSP video capture process is not found or in defunct state,
        then, kill the process and restart it.
        """
        while not self.rtsp_media_instance.check_rtsp_stream():
            logging_to_console_and_syslog("Detected that no RTSP capture "
                                          "process is running. "
                                          "Trying to reopen "
                                          "the RTSP stream..")
            self.rtsp_media_instance.stop_rtsp_stream()
            time.sleep(1)
            self.rtsp_media_instance.start_rtsp_stream()

    def move_media_to_shared_directory(self):
        """
        If you are unable to move the media files to the shared mount,
        then, give it self.max_media_file_skipped_count grace period and then stop and restart rtsp video capture.
        """
        if not self.rtsp_media_instance.move_media_files_to_shared_directory():
            if self.skipped_media_file_creation == self.max_media_file_skipped_count:
                logging_to_console_and_syslog("No mp4 files found."
                                              "Trying to reopen "
                                              "the RTSP stream.")
                self.rtsp_media_instance.stop_rtsp_stream()
                time.sleep(1)
                self.rtsp_media_instance.start_rtsp_stream()
                self.skipped_media_file_creation = 0
            else:
                self.skipped_media_file_creation += 1
                logging_to_console_and_syslog("No mp4 files found."
                                              "Incrementing count to {}"
                                              .format(self.skipped_media_file_creation))

    def perform_operation(self):
        """
        Instruct OpenRTSPAPIHandler class object to start capturing video.
        While this media capture operation is ongoing,
         a. Make sure that the video capture is still ongoing. If it is not active, stop and restart this capture.
         b. Make sure to move the media files periodically into shared media mount.
        """
        message_id = self.rtsp_media_instance.start_rtsp_stream()

        if message_id == 0:
            logging_to_console_and_syslog("Unable to start RTSP stream.")
            raise BaseException

        while True:
            time.sleep(1)
            self.check_and_restart_rtsp_video_capture()
            self.move_media_to_shared_directory()

    def cleanup(self):
        pass


if __name__ == "__main__":
    while True:
        rtsp_recorder_instance = None
        try:
            rtsp_recorder_instance = RtspRecorder()
            while True:
                time.sleep(1)
                rtsp_recorder_instance.perform_operation()
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
            if rtsp_recorder_instance:
                rtsp_recorder_instance.cleanup()
