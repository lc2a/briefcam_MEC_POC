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
        self.read_environment_variables()
        self.connect_to_couchdb_server()
        self.container_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()
        self.document_id = None

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

    def fetch_document_from_database(self,document_id):
        try:
            document = self.database_handle[document_id]
            if document :
                logging_to_console_and_syslog("Found the "
                                              "document {} in the database."
                                              .format(document))
                return document

        except:
            logging_to_console_and_syslog("Document is not found in database.")

        return None

    def update_container_id(self, document_id):
        self.document_id = document_id
        logging_to_console_and_syslog("Trying to update the "
                                      "document id {} in the database.".format(document_id))

        document_dict = self.fetch_document_from_database(document_id)

        if document_dict is None:
            logging_to_console_and_syslog("Document is not found in database.")
            return

        document_dict["container_id"] = self.container_id[:13]
        self.database_handle[document_id] = document_dict

        logging_to_console_and_syslog("Successfully updated the "
                                      "dictionary the database {}"
                                      .format(self.database_handle[document_id]))


    def is_the_document_still_valid(self):
        is_valid = False

        document_dict = self.fetch_document_from_database(self.document_id)

        if document_dict is None:
            logging_to_console_and_syslog("Document is not found in database.")
        else:
            try:
                if document_dict["container_id"] == self.container_id[:13]:
                    logging_to_console_and_syslog("Document is found in database.")
                    is_valid = True
            except:
                logging_to_console_and_syslog("Unable to match the container id in the document.")

        return is_valid
