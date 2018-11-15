import time
import os
import sys
import traceback
from sys import path
import logging
from ast import literal_eval
import unittest
path.append(os.getcwd())
from log.log_file import logging_to_console_and_syslog
from couchdb_client.couchdb_client import CouchDBClient
from rtsp_recorder_orchestrator.orchestrator import RTSPRecorderOrchestrator


class TestCouchDB(unittest.TestCase):
    def setUp(self):
        os.environ["couchdb_server_key"] = "localhost:5984"
        os.environ["id_to_container_name_key"] = "id_to_container"
        os.environ["database_name_key"] = "briefcam"
        #self.create_couchdb_fauxton_front_end_docker_containers()
        self.couchdb_instance = CouchDBClient()
        self.document_id = None
        self.rtsp_orchestrator = None
        os.environ["image_name_key"] = "ssriram1978/rtsp_recorder:latest"
        os.environ["environment_key"] = "video_file_path_key=/data " \
                                        "rtsp_file_name_prefix_key=briefcam " \
                                        "rtsp_duration_of_the_video_key=30 " \
                                        "min_file_size_key=10000000" \
                                        "rtsp_capture_application_key=openRTSP "
        os.environ["bind_mount_key"] = "/var/run/docker.sock:/var/run/docker.sock /usr/bin/docker:/usr/bin/docker"
        self.rtsp_orchestrator = RTSPRecorderOrchestrator()
        self.document_id1 = None
        self.document_id2 = None
        self.document1 = {'name': 'camera1', 'ip': '10.136.66.233'}
        self.document2 = {'name': 'camera2', 'ip': '10.136.66.231'}
        self.container_id1 = None
        self.container_id2 = None

    def create_couchdb_fauxton_front_end_docker_containers(self):
        completedProcess = subprocess.run(["docker-compose", "up", "-d"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)
        time.sleep(120)

    def delete_couchdb_fauxton_front_end_docker_containers(self):
        completedProcess = subprocess.run(["docker-compose", "down"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def create_rows_in_master_db(self):
        self.document_id1 = self.couchdb_instance.add_entry_into_master_database(self.document1)
        self.assertIsNotNone(self.document_id1)
        self.document_id2 = self.couchdb_instance.add_entry_into_master_database(self.document2)
        self.assertIsNotNone(self.document_id2)

    def delete_rows_from_master_db(self):
        self.assertTrue(self.couchdb_instance.delete_entry_from_master_database(self.document_id1))
        self.assertIsNone(self.couchdb_instance.check_entry_in_master_database(self.document_id1))
        self.assertTrue(self.couchdb_instance.delete_entry_from_master_database(self.document_id2))
        self.assertIsNone(self.couchdb_instance.check_entry_in_master_database(self.document_id2))

    def validate_container_id_corresponding_to_document_id(self):
        is_container_id1_found = False
        is_container_id2_found = False

        container_id_dict1 = self.couchdb_instance.fetch_data_from_id_to_container_entry(self.document_id1)
        if container_id_dict1:
            self.container_id1 = container_id_dict1[self.document_id1]
            self.assertIsNotNone(self.container_id1)
            is_container_id1_found = True

        container_id_dict2 = self.couchdb_instance.fetch_data_from_id_to_container_entry(self.document_id2)
        if container_id_dict2:
            self.container_id2 = container_id_dict2[self.document_id2]
            self.assertIsNotNone(self.container_id2)
            is_container_id2_found = True

        self.assertTrue(is_container_id1_found)
        self.assertTrue(is_container_id2_found)

    def validate_no_container_id_corresponding_to_document_id(self):
        self.assertIsNone(self.couchdb_instance.fetch_data_from_id_to_container_entry(self.document_id1))
        self.assertIsNone(self.couchdb_instance.fetch_data_from_id_to_container_entry(self.document_id2))

    def validate_is_container_id_active(self):
        self.assertTrue(self.rtsp_orchestrator.check_if_container_is_active(self.container_id1))
        self.assertTrue(self.rtsp_orchestrator.check_if_container_is_active(self.container_id2))

    def validate_no_active_containers(self):
        self.assertFalse(self.rtsp_orchestrator.check_if_container_is_active(self.container_id1))
        self.assertFalse(self.rtsp_orchestrator.check_if_container_is_active(self.container_id2))

    def add_an_entry_to_couch_db_validate_docker_container_added(self):
        """
        Add a document to couchDB master database.
        Check if couch DB id to document database has a valid container ID.
        Check if there is an active docker container ID in the system.
        """
        self.create_rows_in_master_db()
        #give some time for the docker containers to be created.
        time.sleep(20)
        self.validate_container_id_corresponding_to_document_id()
        self.validate_is_container_id_active()

    def delete_an_entry_from_couch_db_validate_docker_container_removed(self):
        """
        Delete a document from the couch DB master database.
        Check if the corresponding document ID to container ID mapping is deleted from the id to document database.
        Check if the docker container ID is deleted from the system.
        """
        self.delete_rows_from_master_db()
        #give some time for the docker containers to be deleted.
        time.sleep(60)
        self.validate_no_container_id_corresponding_to_document_id()
        self.validate_no_active_containers()

    def validate_self_healing_property(self):
        """
         Force delete an active docker container.
         Check if the front end spawns out a new container and assigns this identifier to the valid document in
         id to document database.
        """
        self.rtsp_orchestrator.stop_container(self.container_id1)
        self.rtsp_orchestrator.stop_container(self.container_id2)
        self.validate_no_active_containers()
        time.sleep(30)
        self.container_id1 = None
        self.container_id2 = None
        self.validate_container_id_corresponding_to_document_id()
        self.validate_is_container_id_active()

    def test_run(self):
        self.assertIsNotNone(self.couchdb_instance)
        self.add_an_entry_to_couch_db_validate_docker_container_added()
        self.validate_self_healing_property()
        self.delete_an_entry_from_couch_db_validate_docker_container_removed()

    def tearDown(self):
        self.couchdb_instance.cleanup()
        self.rtsp_orchestrator.cleanup()
        #self.delete_couchdb_fauxton_front_end_docker_containers()

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