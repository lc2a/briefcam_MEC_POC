import time
import os
import sys
import traceback
from sys import path
import logging
import docker

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog


class RTSPRecorderOrchestrator:
    def __init__(self):
        logging_to_console_and_syslog("Initializing RTSPRecorderOrchestrator instance.")
        self.docker_instance = None
