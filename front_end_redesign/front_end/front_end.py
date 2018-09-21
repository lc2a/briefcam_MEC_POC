import time
import os
import sys
import traceback
from sys import path
import logging
from ast import literal_eval

path.append(os.getcwd())
from log.log_file import logging_to_console_and_syslog
from couchdb_client.couchdb_client import CouchDBClient
from rtsp_recorder_orchestrator.orchestrator import RTSPRecorderOrchestrator


class FrontEnd  :
    def __init__(self):
        logging_to_console_and_syslog("Trying to read the "
                                      "environment variables...")
        self.couchdb_instance = CouchDBClient()
        self.rtsp_recorder_orchestrator = RTSPRecorderOrchestrator()

    def cleanup(self):
        pass

    def create_a_new_container_and_assign_a_job(self, key, data):
        logging_to_console_and_syslog("Creating a new rtsp container for key {} and assign the job {}"
                                      .format(key, data))

        container_id = self.rtsp_recorder_orchestrator.run_container(data)

        if container_id is None:
            logging_to_console_and_syslog("Unable to open a container for data {}"
                                          .format(data))
            raise Exception

        logging_to_console_and_syslog("Adding a new rtsp container {} for  key {} in id to key entry"
                                      .format(container_id, key))

        self.couchdb_instance.add_entry_into_id_to_container_database(key, container_id)

    def validate_and_restart_container_if_needed(self, key, data, container_id):
        logging_to_console_and_syslog("Validating container id {}"
                                      .format(container_id))
        if self.rtsp_recorder_orchestrator.check_if_container_is_active(container_id):
            logging_to_console_and_syslog("container id {} is active."
                                          .format(container_id))
        else:
            self.couchdb_instance.delete_id_to_container_entry(key)
            self.create_a_new_container_and_assign_a_job(key, data)

    def validate_master_db_entries(self):
        for key, data in self.couchdb_instance.yield_database_handle_entries():
            logging_to_console_and_syslog("Found a key {} and data {} in the briefcam database."
                                          .format(key, data))
            container_id_dict = self.couchdb_instance.fetch_data_from_id_to_container_entry(key)
            if container_id_dict:
                container_id = container_id_dict[key]
                logging_to_console_and_syslog("There is a valid container {} "
                                              "for this id {} in id to container db."
                                              .format(container_id, key))
                self.validate_and_restart_container_if_needed(key, data, container_id)
            else:
                self.create_a_new_container_and_assign_a_job(key, data)

    def validate_id_to_container_db_entries(self):
        for key, container_id_dict in self.couchdb_instance.yield_id_to_container_entries():
            container_id = container_id_dict[key]
            logging_to_console_and_syslog("Found a id {} and container id {} in the id to container db"
                                          .format(key, container_id))
            data = self.couchdb_instance.fetch_data_from_master_database(key)
            if data:
                logging_to_console_and_syslog("There is a valid data {} with key {}"
                                              "in the master database"
                                              .format(data, key))
                self.validate_and_restart_container_if_needed(key, data, container_id)
            else:
                logging_to_console_and_syslog("Stopping container {} because the document id {}"
                                              "is no longer present in the master db"
                                              .format(container_id, key))
                self.rtsp_recorder_orchestrator.stop_container(container_id)
                self.couchdb_instance.delete_id_to_container_entry(key)

    def perform_your_job(self):
        #For every document in the briefcam database, check if container id is present in the id to container database.
        #   a. If container ID is present, then, validate the container id
        #          1. If container id is active, then do nothing.
        #          2. If container id is not active, then, create a new new container and update it in the briefcam and
        #             id to container database.
        #   b. If container ID is not present, then, create a new new container and update it in the briefcam and
        #      id to container database.
        #For every document in the id to container database, check if the id is valid in the briefcam database
        #   a. If the id is valid, then, do nothing.
        #   a. If the id is not valid, then, delete the container.
        self.validate_master_db_entries()
        self.validate_id_to_container_db_entries()

if __name__ == "__main__":
    while True:
        front_end_instance = None
        try:
            front_end_instance = FrontEnd()
            while True:
                time.sleep(1)
                front_end_instance.perform_your_job()
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