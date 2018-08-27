import time
import os
import sys
from sys import path
path.append(os.getcwd())

import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler,FileCreatedEvent
import logging

logging.basicConfig(format='(%(threadName)-2s:'
                                       '%(levelname)s:'
                                       '%(asctime)s:'
                                       '%(lineno)d:'
                                       '%(filename)s:'
                                       '%(funcName)s:'
                                       '%(message)s',
                                datefmt='%m/%d/%Y %I:%M:%S %p',
                                filename='directory_watch.log',
                                level=logging.DEBUG)

from kafka_post.kafka_producer import post_filename_to_a_kafka_topic

def process_new_file(file_name):
    #post the file_name into a kafka topic kafka_topic_name
    post_filename_to_a_kafka_topic(file_name)

class UnhandledEventExcept(Exception):
    def __init__(self,arg):
        Exception.__init__(self,arg)
        self.arg=arg

    def __str__(self):
        logging.debug(__class__ + "arg=%s"%(self.arg))

class NewFileEventHandler(FileSystemEventHandler):
    def on_created(self,event):
        if isinstance(event,FileCreatedEvent):
            logging.debug("event.src_path={}".format(event.src_path))
            process_new_file(event.src_path)
            os.remove(event.src_path)
        else:
            raise UnhandledEventExcept(event.__str__)

def watch_a_directory(directory_watch):
    event_handler = NewFileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, directory_watch, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except UnhandledEventExcept as e:
        logging.debug(e)
        observer.stop()
        observer.join()
    except KeyboardInterrupt:
        logging.debug("You pressed ctrl + c")
        observer.stop()
        observer.join()
        raise KeyboardInterrupt
    except OSError:
        logging.debug("OSError raised")
        raise OSError
    finally:
        raise Exception


class NoProcessExcept(Exception):
    def __init__(self,arg):
        Exception.__init__(self,arg)
        self.arg=arg

    def __str__(self):
        logging.debug(__class__ + "arg=%s"%(self.arg))


def watch_a_directory2(video_file_path):
    before = dict([(f, None) for f in os.listdir(video_file_path)])
    while 1:
        time.sleep(1)
        after = dict([(f, None) for f in os.listdir(video_file_path)])
        added = [f for f in after if not f in before]
        removed = [f for f in before if not f in after]
        if added:
            print("Added: ",added)
            logging.debug("Added: ", added)
            for filename in added:
                process_new_file(video_file_path+'/'+filename)
        if removed:
            print("Removed: ", removed)
            logging.debug("Removed: ", removed)
        before = after

if __name__ == "__main__":
    try:
        #if proceed_with_execution() == True:
        video_file_path = os.getenv("video_file_path_key", default=None)
        logging.debug("video_file_path={}".format(video_file_path))
        print("video_file_path={}".format(video_file_path))
        #watch_a_directory(video_file_path)
        watch_a_directory2(video_file_path)
    except NoProcessExcept as e:
        logging.debug("No Process exception occurred.{}".format(e))
    except KeyboardInterrupt:
        logging.debug("You terminated the program by pressing ctrl + c")
    except BaseException:
        logging.debug("Base Exception occurred")
    except:
        logging.debug("Unhandled exception")
    finally:
        exit
