#!/usr/bin/env python3
import time
from kafka import KafkaConsumer
import os
import sys, traceback
import logging
from log.log_file import logging_to_console_and_syslog
from datetime import datetime
