import couchdb
import time
import os
import sys
import traceback
from sys import path

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
from collections import defaultdict
import logging

class CouchDBClient:
    def __init__(self):
        self.couchdb_instance = None
        self.couchdb_server_name = None
        self.database_name = None
        self.database_handle = None
        self.current_list_of_documents = defaultdict(defaultdict)
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

    def read_all_documents_from_the_database(self):
        if self.database_handle is None:
            logging_to_console_and_syslog("database_handle is None.")
            raise BaseException

        for id in self.database_handle:
            documents = self.database_handle[id]
            document = defaultdict(str)
            for name, value in documents.items():
                document[name] = value
            self.current_list_of_documents[id] = document

        logging_to_console_and_syslog("self.current_list_of_documents={}"
                                      .format(repr(self.current_list_of_documents)))
