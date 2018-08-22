import time
import os
import sys
from sys import path
path.append(os.getcwd())

import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler,FileCreatedEvent
from kafka_post.kafka_producer import post_filename_to_a_kafka_topic

def process_new_file(file_name):
    #post the file_name into a kafka topic kafka_topic_name
    post_filename_to_a_kafka_topic(filename)

class UnhandledEventExcept(Exception):
    def __init__(self,arg):
        Exception.__init__(self,arg)
        self.arg=arg

    def __str__(self):
        print(__class__ + "arg=%s"%(self.arg))

class NewFileEventHandler(FileSystemEventHandler):
    def on_created(self,event):
        if isinstance(event,FileCreatedEvent):
            print("event.src_path={}".format(event.src_path))
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
        print(e)
        observer.stop()
        observer.join()
    except KeyboardInterrupt:
        print("You pressed ctrl + c")
        observer.stop()
        observer.join()
        raise KeyboardInterrupt
    except OSError:
        print("OSError raised")
        raise OSError
    finally:
        raise Exception


class NoProcessExcept(Exception):
    def __init__(self,arg):
        Exception.__init__(self,arg)
        self.arg=arg

    def __str__(self):
        print(__class__ + "arg=%s"%(self.arg))


if __name__ == "__main__":
    try:
        #if proceed_with_execution() == True:
        video_file_path = os.getenv("video_file_path_key", default=None)
        print("video_file_path={}".format(video_file_path))
        watch_a_directory(video_file_path)
    except NoProcessExcept as e:
        print("No Process exception occured.{}".format(e))
    except KeyboardInterrupt:
        print("You terminated the program by pressing ctrl + c")
    except BaseException:
        print("Base Exception occured")
    except:
        print("Unhandled exception")
    finally:
        exit
