import time
import os
import sys
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
from infrastructure_components.producer_consumer.producer_consumer import ProducerConsumerAPI
from infrastructure_components.redis_client.redis_interface import RedisInterface


class DirectoryWatch:
    def __init__(self):
        self.before={}
        self.after={}
        self.producer_instance = ProducerConsumerAPI(is_producer=True,
                                                     thread_identifier="Producer",
                                                     type_of_messaging_queue=ProducerConsumerAPI.kafkaMsgQType)
        self.video_file_path = None
        self.redis_instance = RedisInterface("Producer")
        self.load_environment_variables()

    def load_environment_variables(self):
        while self.video_file_path is None:
            time.sleep(1)
            self.video_file_path = os.getenv("video_file_path_key", default=None)
        logging_to_console_and_syslog(("video_file_path={}".format(self.video_file_path)))

    def cleanup(self):
        self.producer_instance.cleanup()

    def process_new_file(self,file_name):
        # post the file_name into the producer queue.
        self.producer_instance.enqueue(file_name)
        event = "Producer: Successfully posted a message = {} into msgQ.".format(file_name)
        self.redis_instance.write_an_event_in_redis_db(event)
        self.redis_instance.increment_enqueue_count()

    def watch_a_directory(self):
        self.before = {}
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
        watch_directory_instance = DirectoryWatch()
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
