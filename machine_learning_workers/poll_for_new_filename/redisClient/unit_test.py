import os
import time
import sys
import traceback
sys.path.append("..")  # Adds higher directory to python modules path.
import unittest
from log.log_file import logging_to_console_and_syslog

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