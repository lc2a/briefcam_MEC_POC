import time
import os
import sys
import traceback
from sys import path
path.append(os.getcwd())
from log.log_file import logging_to_console_and_syslog
from kafka_post.kafka_producer import Producer
from couchdb_client.couchdb_client import CouchDBClient

class FrontEnd:
    def __init__(self):
        logging_to_console_and_syslog("Trying to read the "
                                      "environment variables...")
        self.couchdb_instance = CouchDBClient()
        self.kafka_producer_instance = Producer()

    def check_for_new_documents_and_post_to_kafka(self):
        message = self.couchdb_instance.watch_database_for_entries()
        if message:
            for item in message:
                logging_to_console_and_syslog("Sending {} to kafka.".format(repr(item)))
                self.kafka_producer_instance.post_filename_to_a_kafka_topic(repr(item))

    def cleanup(self):
        self.kafka_producer_instance.close_producer_instance()

if __name__ == "__main__":
    front_end_instance = None
    try:
        front_end_instance = FrontEnd()
        while True:
            time.sleep(1)
            front_end_instance.check_for_new_documents_and_post_to_kafka()
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
        if front_end_instance:
            front_end_instance.cleanup()
        exit
