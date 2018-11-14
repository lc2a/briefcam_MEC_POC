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
    """
    This class does the following:
    1. It talks to couch DB.
    2. It is a slave to the master (front_end class) and performs set of commands dictated by the master.
    """
    def __init__(self):
        self.couchdb_instance = None
        self.couchdb_server_name = None
        self.database_name = None
        self.database_handle = None
        self.id_to_container_db = None
        self.id_to_container_name = None
        self.read_environment_variables()
        self.connect_to_couchdb_server()
        self.create_document_database_name_in_the_server()
        self.create_id_to_container_database()


    def read_environment_variables(self):
        """
        This function sinks in the global environment variables.
        """
        while self.couchdb_server_name is None or \
                self.database_name is None or \
                self.id_to_container_name is None:
            self.couchdb_server_name = os.getenv("couchdb_server_key",
                                                 default=None)
            self.database_name = os.getenv("database_name_key",
                                           default=None)
            self.id_to_container_name = os.getenv("id_to_container_name_key",
                                                  default=None)

        logging_to_console_and_syslog("couchdb_server_name={}"
                                      .format(self.couchdb_server_name))

        logging_to_console_and_syslog("database_name={}"
                                      .format(self.database_name))

        logging_to_console_and_syslog("id_to_container_name={}"
                                      .format(self.id_to_container_name))

    def connect_to_couchdb_server(self):
        """
        This function is used to connect to the couch DB server.
        """
        while self.couchdb_instance is None:
            self.couchdb_instance = couchdb.Server(str("http://{}/".format(self.couchdb_server_name)))
        logging_to_console_and_syslog("Successfully connected to couchdb server {}."
                                      .format(self.couchdb_server_name))

    def create_document_database_name_in_the_server(self):
        """
        This function is used to create document database in the server.
        """
        if self.couchdb_instance is None:
            logging_to_console_and_syslog("couchdb_instance is None.")
            raise BaseException

        if self.database_name in self.couchdb_instance:
            logging_to_console_and_syslog("Found the database name {} in the server."
                                          .format(self.database_name))
            self.database_handle = self.couchdb_instance[self.database_name]
        else:
            logging_to_console_and_syslog("Creating the database name {} in the server."
                                          .format(self.database_name))
            self.database_handle = self.couchdb_instance.create(self.database_name)
            if self.database_handle is None:
                raise BaseException

    def create_id_to_container_database(self):
        """
        This function is used to create id to container database in the server.
        """
        if self.couchdb_instance is None:
            logging_to_console_and_syslog("couchdb_instance is None.")
            raise BaseException

        if self.id_to_container_name in self.couchdb_instance:
            logging_to_console_and_syslog("Found the database name {} in the server."
                                          .format(self.id_to_container_name))
            self.id_to_container_db = self.couchdb_instance[self.id_to_container_name]
        else:
            logging_to_console_and_syslog("Creating the database name {} in the server."
                                          .format(self.id_to_container_name))
            self.id_to_container_db = self.couchdb_instance.create(self.id_to_container_name)

    def yield_database_handle_entries(self):
        """
        This function is used to yield entries from the document database one at a time.
        """
        if self.database_handle is None:
            logging_to_console_and_syslog("database_handle is None.")
            raise BaseException

        for id in self.database_handle:
            yield (id, dict(self.database_handle[id].items()))

    def fetch_data_from_master_database(self, document_id):
        """
        This function is used to fetch a document from the document database based upon the passed in document identifier.
        """
        if document_id is None:
            return None

        document_value = None

        if self.database_handle is None:
            logging_to_console_and_syslog("id_to_container_db is None.")
            raise BaseException

        if document_id in self.database_handle:
            document_value = dict(self.database_handle[document_id].items())
            logging_to_console_and_syslog("Fetching key={}, value={} "
                                          "from master database_handle."
                                          .format(document_id,
                                                  document_value))
        return document_value

    def add_entry_into_master_database(self, document):
        """
        This function is used to add/update a row in the master database.
        """
        if self.database_handle is None:
            logging_to_console_and_syslog("database_handle is None.")
            raise BaseException

        if document is None or type(document) != dict:
            logging_to_console_and_syslog("Invalid input parameters.")
            return None
        try:
            #document = {'type': 'Person', 'name': 'John Doe'}
            doc_id, doc_rev = self.database_handle.save(document)
            logging_to_console_and_syslog("Creating  a document {}, id={} into the master database."
                                          .format(document, doc_id))
            return doc_id
        except:
            return None

    def delete_entry_from_master_database(self, document_id):
        """
        This function is used to delete a row from the master database based upon the key (document id).
        """
        if self.database_handle is None:
            logging_to_console_and_syslog("database_handle is None.")
            raise BaseException

        if document_id in self.database_handle:
            logging_to_console_and_syslog("Deleting document_id {} "
                                          "from database_handle."
                                          .format(document_id))
            del self.database_handle[document_id]
            return True
        return False

    def check_entry_in_master_database(self,document_id):
        """
        This function returns a document value based upon the document identifier from the master database.
        """
        document_value = None
        if document_id is None:
            return None
        if self.database_handle is None:
            logging_to_console_and_syslog("database_handle is None.")
            raise BaseException

        if document_id in self.database_handle:
            document_value = dict(self.database_handle[document_id].items())
            logging_to_console_and_syslog("Fetching key={}, value={} "
                                          "from database_handle."
                                          .format(document_id,
                                                  document_value))
        return document_value

    def add_entry_into_id_to_container_database(self,
                                                document_id,
                                                container_id):
        """
        This function is used to add/update a row in the id to container database.
        """
        if self.id_to_container_db is None:
            logging_to_console_and_syslog("id_to_container_db is None.")
            raise BaseException

        if document_id is None or container_id is None:
            logging_to_console_and_syslog("document_id or container_id is None.")
            return False

        try:
            document = self.id_to_container_db[document_id]
            logging_to_console_and_syslog("Updating {},{} into the id_to_container_database."
                                          .format(document_id, container_id))
            document.update({document_id: container_id})
        except:
            logging_to_console_and_syslog("Adding {},{} into the id_to_container_database."
                                          .format(document_id, container_id))
            self.id_to_container_db[document_id] = {document_id: container_id}

        return True

    def yield_id_to_container_entries(self):
        """
        This function is used to yield each row from id to container db and return it back to the caller one row at a time."
        """
        if self.id_to_container_db is None:
            logging_to_console_and_syslog("id_to_container_db is None.")
            raise BaseException

        for id in self.id_to_container_db:
            yield (id, dict(self.id_to_container_db[id].items()))

    def fetch_data_from_id_to_container_entry(self, document_id):
        """
        This function returns a document value based upon the document identifier from the id to container database.
        """
        document_value = None
        if document_id is None:
            return None
        if self.id_to_container_db is None:
            logging_to_console_and_syslog("id_to_container_db is None.")
            raise BaseException

        if document_id in self.id_to_container_db:
            document_value = dict(self.id_to_container_db[document_id].items())
            logging_to_console_and_syslog("Fetching key={}, value={} "
                                          "from id_to_container_db."
                                          .format(document_id,
                                                  document_value))
        return document_value

    def delete_id_to_container_entry(self, document_id):
        """
        This function is used to delete a row from the id to container database based upon the key (document id).
        """
        if self.id_to_container_db is None:
            logging_to_console_and_syslog("id_to_container_db is None.")
            raise BaseException

        if document_id in self.id_to_container_db:
            logging_to_console_and_syslog("Deleting document_id {} "
                                          "from id_to_container_db."
                                          .format(document_id))
            del self.id_to_container_db[document_id]
            return True
        return False

    def cleanup(self):
        pass