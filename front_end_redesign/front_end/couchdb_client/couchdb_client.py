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
        self.id_to_container_db = None
        self.id_to_container_name = None
        self.read_environment_variables()
        self.connect_to_couchdb_server()
        self.create_document_database_name_in_the_server()
        self.create_id_to_container_database()

    def read_environment_variables(self):
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
        while self.couchdb_instance is None:
            self.couchdb_instance = couchdb.Server(str("http://{}/".format(self.couchdb_server_name)))
        logging_to_console_and_syslog("Successfully connected to couchdb server {}."
                                      .format(self.couchdb_server_name))

    def create_document_database_name_in_the_server(self):
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

    def create_id_to_container_database(self):
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
        if self.database_handle is None:
            logging_to_console_and_syslog("database_handle is None.")
            raise BaseException

        for id in self.database_handle:
            yield (id, dict(self.database_handle[id].items()))

    def fetch_data_from_master_database(self, document_id):
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


    def add_entry_into_id_to_container_database(self,
                                                document_id,
                                                container_id):
        if self.id_to_container_db is None:
            logging_to_console_and_syslog("id_to_container_db is None.")
            raise BaseException

        if document_id is None or container_id is None:
            return

        try:
            document = self.id_to_container_db[document_id]
            logging_to_console_and_syslog("Updating {},{} into the id_to_container_database."
                                          .format(document_id, container_id))
            document.update({document_id: container_id})
        except:
            logging_to_console_and_syslog("Adding {},{} into the id_to_container_database."
                                          .format(document_id, container_id))
            self.id_to_container_db[document_id] = {document_id: container_id}

    def yield_id_to_container_entries(self):
        if self.id_to_container_db is None:
            logging_to_console_and_syslog("id_to_container_db is None.")
            raise BaseException

        for id in self.id_to_container_db:
            yield (id, dict(self.id_to_container_db[id].items()))

    def fetch_data_from_id_to_container_entry(self, document_id):
        document_value = None
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
        if self.id_to_container_db is None:
            logging_to_console_and_syslog("id_to_container_db is None.")
            raise BaseException

        if document_id in self.id_to_container_db:
            logging_to_console_and_syslog("Deleting document_id {} "
                                          "from id_to_container_db."
                                          .format(document_id))
            del self.id_to_container_db[document_id]

    def cleanup(self):
        pass


if __name__ == "__main__":
    # debugging code.
    os.environ["couchdb_server_key"] = "10.1.100.100:5984"
    os.environ["id_to_container_name_key"] = "id_to_container"
    os.environ["database_name_key"] = "briefcam"
    couchdb_instance = None
    try:
        couchdb_instance = CouchDBClient()

        for name, value in couchdb_instance.yield_database_handle_entries():
            logging_to_console_and_syslog("briefcam name = {}, value = {}".format(name, value))
            couchdb_instance.add_entry_into_id_to_container_database(name, "contid")

        for name, value in couchdb_instance.yield_id_to_container_entries():
            logging_to_console_and_syslog("ID to container name = {}, value = {}".format(name, value))

        print("fetch_data_from_id_to_container_entry(junk) returned {}"
              .format(couchdb_instance.fetch_data_from_id_to_container_entry("junk")))

        for name, value in couchdb_instance.yield_database_handle_entries():
            print("fetch_data_from_id_to_container_entry({}) returned {}"
              .format(name, couchdb_instance.fetch_data_from_id_to_container_entry(name)))

        for name, value in couchdb_instance.yield_id_to_container_entries():
            data = couchdb_instance.fetch_data_from_master_database(name)
            logging_to_console_and_syslog("Fetching key = {} from master db resulted {}".format(name, data))

        logging_to_console_and_syslog("Fetching key = xyz from master db resulted {}"
                                      .format(couchdb_instance.fetch_data_from_master_database("xyz")))

        for name, value in couchdb_instance.yield_id_to_container_entries():
            logging_to_console_and_syslog("Deleting key = {}, value = {}".format(name, value))
            couchdb_instance.delete_id_to_container_entry(name)

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
        if couchdb_instance:
            couchdb_instance.cleanup()
