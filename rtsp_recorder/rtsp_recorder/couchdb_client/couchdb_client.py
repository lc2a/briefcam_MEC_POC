import couchdb
import time
import os
import sys
import traceback
from sys import path
sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
from collections import defaultdict

class CouchDBClient:
    def __init__(self):
        self.couchdb_instance = None
        self.couchdb_server_name = None
        self.database_name = None
        self.database_handle = None
        self.before = defaultdict(dict)
        self.after = defaultdict(dict)
        self.read_environment_variables()
        self.connect_to_couchdb_server()

    def read_environment_variables(self):
        while self.couchdb_server_name is None and \
                self.database_name is None:
            time.sleep(2)
            self.couchdb_server_name = os.getenv("couchdb_server_key",
                                                 default=None)
            self.database_name = os.getenv("database_name_key",
                                           default=None)

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

        database_found = False
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

    def populate_dictionary_of_items(self, dict_name):
        for id in self.database_handle:
            dict_of_items = self.database_handle[id]
            for name, value in dict_of_items.items():
                dict_name[id][name] = value

    def watch_database_for_entries(self):
        return_list = None
        if self.database_handle is None:
            logging_to_console_and_syslog("database_handle is None.")
            raise BaseException

        self.populate_dictionary_of_items(self.after)
        added = [f for f in self.after.items() if not f in self.before.items()]
        removed = [f for f in self.before.items() if not f in self.after.items()]
        if added:
            logging_to_console_and_syslog("Added: " + str(added))
            return_list = added
        if removed:
            logging_to_console_and_syslog("Removed: " + str(removed))
        self.before = self.after
        self.after = defaultdict(dict)
        return return_list

    def update_container_id(self,message):

    def is_the_document_still_valid(self,message):