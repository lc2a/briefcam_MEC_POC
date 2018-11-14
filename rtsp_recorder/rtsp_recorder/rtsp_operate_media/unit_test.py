import os
import time
import sys
import traceback

sys.path.append("..")  # Adds higher directory to python modules path.
import unittest
from log.log_file import logging_to_console_and_syslog
from couchdb_client import CouchDBClient

class TestCouchDBClient(unittest.TestCase):
    def setUp(self):
        os.environ["couchdb_server_key"] = "10.1.100.100:5984"
        os.environ["id_to_container_name_key"] = "id_to_container"
        os.environ["database_name_key"] = "briefcam"
        couchdb_instance = CouchDBClient()
        pass

    def tearDown(self):
        if couchdb_instance:
            couchdb_instance.cleanup()

if __name__ == "__main__":
    # debugging code.
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