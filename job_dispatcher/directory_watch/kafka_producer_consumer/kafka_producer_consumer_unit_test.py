import os
import time
import sys
import traceback
import unittest
import subprocess
from kafka_producer import Producer
from kafka_consumer import Consumer
import threading



sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog


class ConsumerThread(threading.Thread):
    def __init__(self, id, name):
        threading.Thread.__init__(self)
        self.threadID = id
        self.threadName = name

    def run(self):
        logging_to_console_and_syslog("Starting {}".format(self.threadName))
        kafka_consumer_instance = Consumer()
        t = threading.currentThread()
        while getattr(t, "do_run", True):
            #logging_to_console_and_syslog("Consumer {}. Connecting and polling for new message."
             #                             .format(self.threadName))
            kafka_consumer_instance.connect_and_poll_for_new_message(self.threadName)
        logging_to_console_and_syslog("Exiting {}".format(self.threadName))


class TestWurstmeisterKafka(unittest.TestCase):
    def setUp(self):
        os.environ["broker_name_key"] = "localhost:9094"
        os.environ["topic_key"] = "video-file-name"
        os.environ["redis_log_keyname_key"] = "redis_log"
        os.environ["total_produced_count_redis_name_key"] = "total_produced_count"
        os.environ["total_consumed_count_redis_name_key"] = "total_consumed_count"
        os.environ["redis_server_hostname_key"] = "localhost"
        os.environ["redis_server_port_key"] = "6379"
        self.max_consumer_threads = 10
        self.create_test_kafka_docker_container()
        self.kafka_producer_instance = Producer()
        self.consumer_threads = None
        self.create_consumer_threads()

    def create_consumer_threads(self):
        self.consumer_threads = [0] * self.max_consumer_threads
        for index in range(self.max_consumer_threads):
            self.consumer_threads[index] = ConsumerThread(index,
                                                          "{}{}".format("thread", index))
            self.consumer_threads[index].do_run = True
            self.consumer_threads[index].start()

    def test_run(self):
        logging_to_console_and_syslog("Validating producer instance to be not null.")
        self.assertIsNotNone(self.kafka_producer_instance)

        logging_to_console_and_syslog("Validating consumer threads to be not null.")
        for index in range(self.max_consumer_threads):
            self.assertIsNotNone(self.consumer_threads[index])

        time.sleep(20)

        logging_to_console_and_syslog("Posting messages to kafkaQ.")
        self.assertTrue(self.post_messages_to_kafka())

        time.sleep(60)

        logging_to_console_and_syslog("Validating if the consumer successfully dequeued messages.")
        kafka_consumer_instance = Consumer()
        self.assertEqual(self.kafka_producer_instance.get_current_job_count(),
                         kafka_consumer_instance.get_current_job_count())

    def post_messages_to_kafka(self):
        messages = [str(x) for x in range(10)]
        for message in messages:
            self.kafka_producer_instance.post_message_to_a_kafka_topic(message)
        return True

    def create_test_kafka_docker_container(self):
        completedProcess = subprocess.run(["docker-compose",
                                           "-f",
                                           "docker-compose_wurstmeister_kafka.yml",
                                           "up",
                                           "-d"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)
        # time.sleep(120)

    def delete_test_kafka_docker_container(self):
        completedProcess = subprocess.run(["docker-compose",
                                           "-f",
                                           "docker-compose_wurstmeister_kafka.yml",
                                           "down"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def tearDown(self):
        self.kafka_producer_instance.cleanup()
        self.delete_test_kafka_docker_container()
        time.sleep(5)
        for index in range(self.max_consumer_threads):
            self.consumer_threads[index].do_run = False
            time.sleep(1)
            self.consumer_threads[index].join(1.0)
            if self.consumer_threads[index].is_alive():
                try:
                    self.consumer_threads[index]._stop()

                except:
                    logging_to_console_and_syslog("Caught an exception while stopping thread {}"
                                                  .format(self.consumer_threads[index].getName()))
        sys.exit()

if __name__ == "__main__":
    unittest.main()
