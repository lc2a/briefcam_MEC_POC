import os
import time
import sys
import traceback
import subprocess
import unittest

def import_all_packages():
    realpath=os.path.realpath(__file__)
    #print("os.path.realpath({})={}".format(__file__,realpath))
    dirname=os.path.dirname(realpath)
    #print("os.path.dirname({})={}".format(realpath,dirname))
    dirname_list=dirname.split('/')
    #print(dirname_list)
    for index in range(len(dirname_list)):
        module_path='/'.join(dirname_list[:index])
        #print("module_path={}".format(module_path))
        try:
            sys.path.append(module_path)
        except:
            #print("Invalid module path {}".format(module_path))
            pass

import_all_packages()


from log.log_file import logging_to_console_and_syslog
from couchdb_client.couchdb_client import CouchDBClient

#unit tests
class TestCouchDB(unittest.TestCase):
    def setUp(self):
        os.environ["couchdb_server_key"] = "localhost:5984"
        os.environ["id_to_container_name_key"] = "id_to_container"
        os.environ["database_name_key"] = "briefcam"
        self.create_couchdb_fauxton_docker_container()
        self.couchdb_instance = CouchDBClient()
        self.document_id1 = None
        self.document_id2 = None
        self.document1 = {'type': 'Person1', 'name': 'John Doe'}
        self.document2 = {'type': 'Person2', 'name': 'Ashley'}

    def test_run(self):
        logging_to_console_and_syslog("Validating couchdb_instance to be not null.")
        self.assertIsNotNone(self.couchdb_instance)
        self.create_rows_in_master_db()
        self.create_rows_in_id_to_container_db()
        self.validate_data_from_master_db()
        self.fetch_rows_from_master_db()
        self.fetch_rows_from_id_to_container_db()

    def create_couchdb_fauxton_docker_container(self):
        completedProcess = subprocess.run(["docker-compose", "up", "-d"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)
        time.sleep(120)

    def create_rows_in_master_db(self):
        logging_to_console_and_syslog("Unit testing function add_entry_into_master_database()")
        self.document_id1 = self.couchdb_instance.add_entry_into_master_database(self.document1)
        self.assertIsNotNone(self.document_id1)

        logging_to_console_and_syslog("Unit testing function add_entry_into_master_database()")
        self.document_id2 = self.couchdb_instance.add_entry_into_master_database(self.document2)
        self.assertIsNotNone(self.document_id2)

    def fetch_rows_from_master_db(self):
        logging_to_console_and_syslog("Unit testing function fetch_data_from_master_database()")
        self.assertIsNotNone(self.couchdb_instance.fetch_data_from_master_database(self.document_id1))
        logging_to_console_and_syslog("Unit testing function fetch_data_from_master_database()")
        self.assertIsNotNone(self.couchdb_instance.fetch_data_from_master_database(self.document_id2))

    def validate_data_from_master_db(self):
        document_id1_found = False
        document_id2_found = False
        logging_to_console_and_syslog("Unit testing function yield_database_handle_entries()")
        for key, data in self.couchdb_instance.yield_database_handle_entries():
            self.assertIsNotNone(key)
            self.assertIsNotNone(data)
            if key == self.document_id1:
                document_id1_found = True
            if key == self.document_id2:
                document_id2_found = True

        self.assertTrue(document_id1_found)
        self.assertTrue(document_id2_found)

    def create_rows_in_id_to_container_db(self):
        logging_to_console_and_syslog("Unit testing function add_entry_into_id_to_container_database()")
        self.assertTrue(self.couchdb_instance.add_entry_into_id_to_container_database(self.document_id1, "2345"))
        logging_to_console_and_syslog("Unit testing function add_entry_into_id_to_container_database()")
        self.assertTrue(self.couchdb_instance.add_entry_into_id_to_container_database(self.document_id1, "2345"))
        logging_to_console_and_syslog("Unit testing function add_entry_into_id_to_container_database()")
        self.assertTrue(self.couchdb_instance.add_entry_into_id_to_container_database(self.document_id2, "3456"))
        logging_to_console_and_syslog("Unit testing function add_entry_into_id_to_container_database()")
        self.assertFalse(self.couchdb_instance.add_entry_into_id_to_container_database(self.document_id1, None))
        logging_to_console_and_syslog("Unit testing function add_entry_into_id_to_container_database()")
        self.assertFalse(self.couchdb_instance.add_entry_into_id_to_container_database(None, "2345"))

    def fetch_rows_from_id_to_container_db(self):
        document_id1_found = False
        document_id2_found = False
        logging_to_console_and_syslog("Unit testing function yield_id_to_container_entries()")
        for key, container_id_dict in self.couchdb_instance.yield_id_to_container_entries():
            self.assertIsNotNone(key)
            self.assertIsNotNone(container_id_dict)
            container_id = container_id_dict[key]
            self.assertIsNotNone(container_id)
            if key == self.document_id1:
                document_id1_found = True
            if key == self.document_id2:
                document_id2_found = True
        self.assertTrue(document_id1_found)
        self.assertTrue(document_id2_found)
        logging_to_console_and_syslog("Unit testing function fetch_data_from_id_to_container_entry()")
        self.assertIsNone(self.couchdb_instance.fetch_data_from_id_to_container_entry("junk"))
        logging_to_console_and_syslog("Unit testing function fetch_data_from_id_to_container_entry()")
        self.assertIsNotNone(self.couchdb_instance.fetch_data_from_id_to_container_entry(self.document_id1))
        logging_to_console_and_syslog("Unit testing function fetch_data_from_id_to_container_entry()")
        self.assertIsNotNone(self.couchdb_instance.fetch_data_from_id_to_container_entry(self.document_id2))

    def delete_rows(self):
        logging_to_console_and_syslog("Unit testing function delete_entry_from_master_database()")
        self.assertTrue(self.couchdb_instance.delete_entry_from_master_database(self.document_id1))
        logging_to_console_and_syslog("Unit testing function delete_entry_from_master_database()")
        self.assertTrue(self.couchdb_instance.delete_entry_from_master_database(self.document_id2))
        logging_to_console_and_syslog("Unit testing function delete_entry_from_master_database()")
        self.assertFalse(self.couchdb_instance.delete_entry_from_master_database(self.document_id2))
        logging_to_console_and_syslog("Unit testing function delete_entry_from_master_database()")
        self.assertTrue(self.couchdb_instance.delete_id_to_container_entry(self.document_id1))
        logging_to_console_and_syslog("Unit testing function delete_entry_from_master_database()")
        self.assertTrue(self.couchdb_instance.delete_id_to_container_entry(self.document_id2))
        logging_to_console_and_syslog("Unit testing function delete_entry_from_master_database()")
        self.assertFalse(self.couchdb_instance.delete_id_to_container_entry(self.document_id2))
        logging_to_console_and_syslog("Unit testing function delete_entry_from_master_database()")
        self.assertFalse(self.couchdb_instance.delete_id_to_container_entry("junk"))

    def delete_couchdb_fauxton_docker_container(self):
        completedProcess = subprocess.run(["docker-compose", "down"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def tearDown(self):
        self.delete_rows()
        self.couchdb_instance.cleanup()
        self.delete_couchdb_fauxton_docker_container()


if __name__ == "__main__":
    unittest.main()
