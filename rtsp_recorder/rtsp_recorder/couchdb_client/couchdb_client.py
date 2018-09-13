import couchdb
import time
import os
import sys, traceback
from sys import path

path.append(os.getcwd())
from log.log_file import logging_to_console_and_syslog
from collections import defaultdict


class CouchDBClient:
    def __init__(self):
        self.couchdb_client_instance = None
        self.couchdb_server_name = None
        self.database_name = None
        self.database_handle = None
        self.read_environment_variables()
        self.connect_to_couchdb_server()

    def read_environment_variables(self):
        while self.couchdb_server_name is None and \
                self.database_name is None:
            time.sleep(2)
            logging_to_console_and_syslog("Trying to read the "
                                          "environment variables...")
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
        if self.database_handle is None:
            logging_to_console_and_syslog("database_handle is None.")
            raise BaseException
