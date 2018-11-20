import time
import os
import sys
from sys import path
path.append(os.getcwd())

from kafka_producer_consumer.kafka_producer import Producer
from log.log_file import logging_to_console_and_syslog
#from collections import defaultdict

class Directory_watch:
    def __init__(self):
        self.before={}
        self.after={}
        self.producer_instance=Producer()
        self.video_file_path = None
        while self.video_file_path is None:
            self.video_file_path = os.getenv("video_file_path_key", default=None)
        logging_to_console_and_syslog(("video_file_path={}".format(self.video_file_path)))

    def cleanup(self):
        self.producer_instance.close_producer_instance()

    def process_new_file(self,file_name):
        # post the file_name into a kafka topic kafka_topic_name
        self.producer_instance.post_message(file_name)

    def watch_a_directory(self):
        self.before = dict([(f, None) for f in os.listdir(self.video_file_path)])
        while True:
            time.sleep(1)
            self.after = dict([(f, None) for f in os.listdir(self.video_file_path)])
            added = [f for f in self.after if not f in self.before]
            removed = [f for f in self.before if not f in self.after]
            if added:
                logging_to_console_and_syslog("Added: " + str(added))
                for filename in added:
                    self.process_new_file(self.video_file_path + '/' + filename)
            if removed:
                logging_to_console_and_syslog("Removed: " + str(removed))
            self.before = self.after


if __name__ == "__main__":
    watch_directory_instance = None
    try:
        watch_directory_instance = Directory_watch()
        watch_directory_instance.watch_a_directory()
    except KeyboardInterrupt:
        logging_to_console_and_syslog("You terminated the program by pressing ctrl + c")
    except BaseException:
        logging_to_console_and_syslog("Base Exception occurred {}.".format(sys.exc_info()[0]))
    except:
        logging_to_console_and_syslog("Unhandled exception {}.".format(sys.exc_info()[0]))
    finally:
        if watch_directory_instance:
            watch_directory_instance.cleanup()
        exit
