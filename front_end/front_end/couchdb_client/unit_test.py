import os
import time
import sys
import traceback
import unittest
import subprocess
from couchdb_client import CouchDBClient

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog

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

        self.document_id1 = self.couchdb_instance.add_entry_into_master_database(self.document1)
        self.assertIsNotNone(self.document_id1)

        self.document_id2 = self.couchdb_instance.add_entry_into_master_database(self.document2)
        self.assertIsNotNone(self.document_id2)

    def fetch_rows_from_master_db(self):
        self.assertIsNotNone(self.couchdb_instance.fetch_data_from_master_database(self.document_id1))
        self.assertIsNotNone(self.couchdb_instance.fetch_data_from_master_database(self.document_id2))

    def validate_data_from_master_db(self):
        document_id1_found = False
        document_id2_found = False
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
        self.assertTrue(self.couchdb_instance.add_entry_into_id_to_container_database(self.document_id1, "2345"))
        self.assertTrue(self.couchdb_instance.add_entry_into_id_to_container_database(self.document_id1, "2345"))
        self.assertTrue(self.couchdb_instance.add_entry_into_id_to_container_database(self.document_id2, "3456"))
        self.assertFalse(self.couchdb_instance.add_entry_into_id_to_container_database(self.document_id1, None))
        self.assertFalse(self.couchdb_instance.add_entry_into_id_to_container_database(None, "2345"))

    def fetch_rows_from_id_to_container_db(self):
        document_id1_found = False
        document_id2_found = False
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
        self.assertIsNone(self.couchdb_instance.fetch_data_from_id_to_container_entry("junk"))
        self.assertIsNotNone(self.couchdb_instance.fetch_data_from_id_to_container_entry(self.document_id1))
        self.assertIsNotNone(self.couchdb_instance.fetch_data_from_id_to_container_entry(self.document_id2))

    def delete_rows(self):
        self.assertTrue(self.couchdb_instance.delete_entry_from_master_database(self.document_id1))
        self.assertTrue(self.couchdb_instance.delete_entry_from_master_database(self.document_id2))
        self.assertFalse(self.couchdb_instance.delete_entry_from_master_database(self.document_id2))
        self.assertTrue(self.couchdb_instance.delete_id_to_container_entry(self.document_id1))
        self.assertTrue(self.couchdb_instance.delete_id_to_container_entry(self.document_id2))
        self.assertFalse(self.couchdb_instance.delete_id_to_container_entry(self.document_id2))
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
    try:
        unittest.main()
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
        pass