#!/usr/bin/env python3
import time
import os
import sys
import traceback
from sys import path

path.append(os.getcwd())
from log.log_file import logging_to_console_and_syslog
from rtsp_operate_media.rtsp_operate_media import RtspOperationsOnMedia

class RtspRecorder:
    def __init__(self):
        self.rtsp_media_instance = None
        self.initialize_instances()
        self.rtsp_message = None

    def initialize_instances(self):
        self.rtsp_media_instance = RtspOperationsOnMedia()

    def perform_operation(self):
        # 2. Instruct RTSP to start capturing video.
        message_id = self.rtsp_media_instance.start_rtsp_stream()

        if message_id == 0:
            logging_to_console_and_syslog("Unable to start RTSP stream.")
            raise BaseException

        no_mp4_files_found_count = 0
        while True:
            # 1. Make sure that the video capture is still ongoing.
            # 2. Make sure to move the media files periodically into shared media mount.
            time.sleep(1)
            while not self.rtsp_media_instance.check_rtsp_stream():
                logging_to_console_and_syslog("Detected that no RTSP capture "
                                              "process is running. "
                                              "Trying to reopen "
                                              "the RTSP stream..")
                self.rtsp_media_instance.stop_rtsp_stream()
                time.sleep(1)
                self.rtsp_media_instance.start_rtsp_stream()
            if not self.rtsp_media_instance.move_media_files_to_shared_directory():
                if no_mp4_files_found_count == 5:
                    logging_to_console_and_syslog("No mp4 files found."
                                                  "Trying to reopen "
                                                  "the RTSP stream.")
                    self.rtsp_media_instance.stop_rtsp_stream()
                    time.sleep(1)
                    self.rtsp_media_instance.start_rtsp_stream()
                    no_mp4_files_found_count = 0
                else:
                    no_mp4_files_found_count += 1
                    logging_to_console_and_syslog("No mp4 files found."
                                                  "Incrementing count to {}"
                                                  .format(no_mp4_files_found_count))

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
