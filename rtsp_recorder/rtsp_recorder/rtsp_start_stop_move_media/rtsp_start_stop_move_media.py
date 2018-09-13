import os
import sys, traceback
from sys import path
sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog

class RtspStartStopMoveMedia:
    def __init__(self):
        self.process_instance=None

    def start_rtsp_stream(self,message):

    def stop_rtsp_stream(self,message):

    def check_rtsp_stream(self,message):

    def move_media_files_to_shared_directory(self):
