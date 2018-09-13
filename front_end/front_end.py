import couchdb
import time
import os
import sys, traceback
from sys import path
path.append(os.getcwd())
from kafka_post.kafka_producer import Producer
from log.log_file import logging_to_console_and_syslog
from collections import defaultdict

class Front_end:
    def __init__(self):
        self.couchdb_instance=None
        self.broker_name=None
        self.topic=None
        self.couchdb_server_name=None
        self.database_name=None
        self.database_handle=None
        self.before = defaultdict(dict)
        self.after = defaultdict(dict)
        self.producer_instance = Producer()
        self.read_environment_variables()
        self.connect_to_couchdb_server()
        self.watch_database_for_entries()

    def read_environment_variables(self):
        while self.topic is None  and \
            self.couchdb_server_name is None and \
            self.broker_name is None and \
            self.database_name is None :
            time.sleep(2)
            logging_to_console_and_syslog("Trying to read the "
                                          "environment variables...")
            self.broker_name = os.getenv("broker_name_key",
                                   default=None)
            self.topic = os.getenv("topic_key",
                                         default=None)
            self.couchdb_server_name = os.getenv("couchdb_server_key",
                                                   default=None)
            self.database_name = os.getenv("database_name_key",
                                                   default=None)

        logging_to_console_and_syslog("broker_name={}"
                                      .format(self.broker_name))

        logging_to_console_and_syslog("topic={}"
                                  .format(self.topic))

        logging_to_console_and_syslog("couchdb_server_name={}"
                                      .format(self.couchdb_server_name))

        logging_to_console_and_syslog("database_name={}"
                                      .format(self.database_name))

    def connect_to_couchdb_server(self):
        while self.couchdb_instance is None:
            time.sleep(2)
            self.couchdb_instance = couchdb.Server(str("http://{}/".format(self.couchdb_server_name)))
        logging_to_console_and_syslog("Successfully connected to couchdb server {}."
                                      .format(self.couchdb_server_name))

        database_found=False
        while database_found is False:
            if self.database_name in self.couchdb_instance:
                self.database_handle = self.couchdb_instance[self.database_name]
                logging_to_console_and_syslog("Successfully Found Database name {} in server {}."
                                          .format(self.database_name,
                                                  self.couchdb_server_name))
                database_found = True
            else:
                logging_to_console_and_syslog("Unable to find database name {} in server {}."
                                          .format(self.database_name,
                                                  self.couchdb_server_name))
                time.sleep(5)

    def cleanup(self):
        self.producer_instance.close_producer_instance()

    def process_new_file(self,file_name):
        # post the file_name into a kafka topic kafka_topic_name
        self.producer_instance.post_filename_to_a_kafka_topic(file_name)

    def populate_dictionary_of_items(self, dict_name):
        for id in self.database_handle:
            dict_of_items = self.database_handle[id]
            for name, value in dict_of_items.items():
                dict_name[id][name]=value

    def watch_database_for_entries(self):
        if self.database_handle is None:
            logging_to_console_and_syslog("database_handle is None.")
            raise BaseException
        while True:
            time.sleep(1)
            self.populate_dictionary_of_items(self.after)
            added = [f for f in self.after.items() if not f in self.before.items()]
            removed = [f for f in self.before.items() if not f in self.after.items()]
            if added:
                logging_to_console_and_syslog("Added: " + str(added))
                for server_info in added:
                    self.process_new_file(repr(server_info))
            if removed:
                logging_to_console_and_syslog("Removed: " + str(removed))
            self.before = self.after
            self.after=defaultdict(dict)

if __name__ == "__main__":
    front_end_instance = None
    try:
        front_end_instance = Front_end()
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
