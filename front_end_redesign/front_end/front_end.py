import time
import os
import sys
import traceback
from sys import path
import logging

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

    def perform_your_job(self):
        self.couchdb_instance.read_all_documents_from_the_database()


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