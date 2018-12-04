import time
import os
import sys
import traceback


#path.append(os.getcwd())

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

from infrastructure_components.log.log_file import logging_to_console_and_syslog
from infrastructure_components.couchdb_client.couchdb_client import CouchDBClient
from tier2.front_end.rtsp_docker_orchestrator.orchestrator import RTSPDockerOrchestrator


class FrontEnd:
    """
    This is a Behavioral Iterator design pattern. https://sourcemaking.com/design_patterns
    This class front_end does the following.
    1. Talk to couchdb_client class object to monitor for new entries in the Couch DB.
    2. Talk to RTSP docker container orchestrator class object and hand over the newly programmed entries in Couch DB.
    3. Perform book keeping task to make the couch DB instance in sync with the orchestrator class object.
    """

    """
    1. Instantiate couch DB instance.
    2. Instantiate RTSP Recorder Orchestrator instance.
    """
    def __init__(self):
        logging_to_console_and_syslog("Trying to read the "
                                      "environment variables...")
        self.couchdb_instance = CouchDBClient()
        self.rtsp_recorder_orchestrator = RTSPDockerOrchestrator()

    def cleanup(self):
        """
        1. Cleanup function. Right now, there is nothing to clean up.
        """
        pass

    def create_a_new_container_and_assign_a_job(self, key, data):
        """
        This function creates a new docker container with the input data as the container argument.
        """
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
        """
        This function checks if the provided docker container identifier is active.
        1. If it is active, it just logs a message and exits.
        2. If the container is not active, it make sures to delete the container identifier from couch DB and then
        creates a new RTSP docker container instance with the data as the container additional parameter.
        """
        logging_to_console_and_syslog("Validating container id {}"
                                      .format(container_id))
        if self.rtsp_recorder_orchestrator.check_if_container_is_active(container_id):
            logging_to_console_and_syslog("container id {} is active."
                                          .format(container_id))
        else:
            self.couchdb_instance.delete_id_to_container_entry(key)
            self.create_a_new_container_and_assign_a_job(key, data)

    def validate_master_db_entries(self):
        """
        This is a book keeping function.
        This function does the following:
        1. It walks through each and every document in the couch DB and gets the document Identifier.
        2. For each document identifier, it checks if there is a corresponding container identifier in the database.
            a. For each docker container identifier in the database, it checks if there is a corresponding
               docker container that is alive.
                1. If the docker container is alive, then it just logs a message.
                2. If the docker container is not alive, then it deletes the docker identifier from database and
                start a new docker container with the passed in data as the container argument.
        """
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
        """
        This is second book keeping task.
        When a document is deleted from the couch DB database, the corresponding docker RTSP container instance has to be
        deleted.
        For every document identifier to docker container identifier entry in the couch DB,
            1. Check if the document identifier is present in the couch DB document to data table.
                a. If the document identifier is present, then, validate if the docker container is still running.
                b. If the document idenfier is not present, then, delete the corresponding RTSP docker container instance.
        """
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
        """
        For every document in the briefcam database, check if container id is present in the id to container database.
          a. If container ID is present, then, validate the container id
                 1. If container id is active, then do nothing.
                 2. If container id is not active, then, create a new new container and update it in the briefcam and
                    id to container database.
          b. If container ID is not present, then, create a new new container and update it in the briefcam and
             id to container database.
        For every document in the id to container database, check if the id is valid in the briefcam database
          a. If the id is valid, then, do nothing.
          a. If the id is not valid, then, delete the container.
        """
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
