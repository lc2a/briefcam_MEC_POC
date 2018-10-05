import os
import time
import sys

sys.path.append("..")  # Adds higher directory to python modules path.
import redis
from log.log_file import logging_to_console_and_syslog
import logging
import traceback
import unittest
from datetime import datetime

class RedisClient(object):
    __instance = None

    def __new__(cls):
        if RedisClient.__instance is None:
            RedisClient.__instance = object.__new__(cls)
        return RedisClient.__instance

    def __init__(self):
        self.redis_instance = None
        self.redis_server_hostname = None
        self.redis_server_port = 0
        self.load_environment_variables()
        self.connect_to_redis_server()
        self.hostname = os.popen("cat /etc/hostname").read()
        self.cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()
        self.cont_id = self.cont_id[:12]

    def load_environment_variables(self):
        while self.redis_server_hostname is None or \
                self.redis_server_port == 0:
            time.sleep(2)
            logging_to_console_and_syslog("Trying to read environment variables...")
            self.redis_server_hostname = os.getenv("redis_server_hostname_key",
                                                   default=None)
            self.redis_server_port = int(os.getenv("redis_server_port_key",
                                                   default=0))

        logging_to_console_and_syslog("redis_server_hostname={}"
                                      .format(self.redis_server_hostname),
                                      logging.INFO)
        logging_to_console_and_syslog("redis_server_port={}"
                                      .format(self.redis_server_port),
                                      logging.INFO)

    def connect_to_redis_server(self):
        while self.redis_instance is None:
            self.redis_instance = redis.StrictRedis(host=self.redis_server_hostname,
                                                    port=self.redis_server_port,
                                                    db=0)
        logging_to_console_and_syslog("Successfully connected "
                                      "to redisClient server {},port {}"
                                      .format(self.redis_server_hostname,
                                              self.redis_server_port),
                                      logging.INFO)

    def write_an_event_on_redis_db(self, event,key=None):
        return_value = False
        if self.redis_instance is not None:
            current_time = datetime.now()
            event_string = "\n Time={},Hostname={},containerID={},event={}"\
                .format(str(current_time),
                        self.hostname,
                        self.cont_id[:12],
                        event)
            key_name = None
            if key:
                key_name = key
            else:
                key_name = self.cont_id
            if self.redis_instance.exists(key_name):
                self.redis_instance.append(key_name, event_string)
                logging_to_console_and_syslog("Appending {} to {}".format(event_string, key_name))
                return_value = True
            else:
                self.redis_instance.set(key_name, event_string)
                logging_to_console_and_syslog("Writing {} to {}".format(event_string, key_name))
                return_value = True
        return return_value

    def check_if_the_key_exists_in_redis_db(self, key):
        return_value = False
        if self.redis_instance is not None:
            if self.redis_instance.exists(key):
                return_value = True
        return return_value

    def set_the_key_in_redis_db(self, key):
        return_value = False
        if self.redis_instance is not None:
            self.redis_instance.set(key, 1)
            return_value = True
        return return_value

    def delete_key_from_redis_db(self, key):
        return_value = False
        if self.redis_instance is not None:
            if self.redis_instance.exists(key):
                if self.redis_instance.delete(key):
                    return_value = True
        return return_value

    def increment_key_in_redis_db(self,key):
        return_value = False
        if self.redis_instance is not None:
            self.redis_instance.incr(key)
            return_value = True
        return return_value

    def read_key_value_from_redis_db(self,key):
        return_value = -1
        if self.redis_instance is not None:
            if self.redis_instance.exists(key):
                return_value = self.redis_instance.get(key)
        return return_value

    def cleanup(self):
        pass

#unit tests
class TestRedisClient(unittest.TestCase):
    def setUp(self):
        os.environ["redis_server_hostname_key"] = "10.2.40.162"
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["redis_server_port_key"] = "6379"
        self.redisClient = RedisClient()

    def test_key_in_redis(self):
        redisClient_inst1 = RedisClient()
        self.assertTrue(redisClient_inst1.set_the_key_in_redis_db("abcxyz"))
        self.assertTrue(redisClient_inst1.check_if_the_key_exists_in_redis_db("abcxyz"))

    def test_delete_key_in_redis(self):
        redisClient_inst1 = RedisClient()
        self.assertTrue(redisClient_inst1.set_the_key_in_redis_db("abcxyz1"))
        self.assertTrue(redisClient_inst1.delete_key_from_redis_db("abcxyz1"))
        self.assertFalse(redisClient_inst1.check_if_the_key_exists_in_redis_db("abcxyz1"))

    def test_singleton(self):
        redisClient_inst1 = RedisClient()
        redisClient_inst2 = RedisClient()
        self.assertEqual(redisClient_inst1, redisClient_inst2)
        self.assertEqual(self.redisClient, redisClient_inst1)

    def test_redis_connection(self):
        redisClient_inst1 = RedisClient()
        self.assertIsNotNone(redisClient_inst1.redis_instance)

    def test_write_event(self):
        redisClient_inst1 = RedisClient()
        redisClient_inst1.cont_id="12345"
        self.assertTrue(redisClient_inst1.write_an_event_on_redis_db("Hello world"))
        self.assertTrue(redisClient_inst1.write_an_event_on_redis_db("Hello world2"))
        self.assertTrue(redisClient_inst1.check_if_the_key_exists_in_redis_db(redisClient_inst1.cont_id))

    def test_incr_key(self):
        redisClient_inst1 = RedisClient()
        self.assertTrue(redisClient_inst1.increment_key_in_redis_db("incr_key"))
        self.assertEqual(redisClient_inst1.read_key_value_from_redis_db("incr_key"),b'1')
        self.assertTrue(redisClient_inst1.increment_key_in_redis_db("incr_key"))
        self.assertEqual(redisClient_inst1.read_key_value_from_redis_db("incr_key"),b'2')
        self.assertTrue(redisClient_inst1.increment_key_in_redis_db("incr_key"))
        self.assertEqual(redisClient_inst1.read_key_value_from_redis_db("incr_key"),b'3')

    def tearDown(self):
        self.redisClient.delete_key_from_redis_db("abcxyz")
        self.redisClient.delete_key_from_redis_db("incr_key")
        self.redisClient.delete_key_from_redis_db(self.redisClient.cont_id)

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
