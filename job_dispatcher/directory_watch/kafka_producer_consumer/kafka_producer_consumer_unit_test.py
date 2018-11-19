import os
import time
import sys
import traceback
import unittest
import subprocess
from kafka_producer import Producer
from kafka_consumer import Consumer

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog

class TestWurstmeisterKafka(unittest.TestCase):
    def setUp(self):
        os.environ["broker_name_key"] = "localhost:9094"
        os.environ["topic_key"] = "video-file-name"
        os.environ["redis_log_keyname_key"] = "redis_log"
        os.environ["total_produced_count_redis_name_key"] = "total_produced_count"
        os.environ["total_consumed_count_redis_name_key"] = "total_consumed_count"
        os.environ["redis_server_hostname_key"] = "localhost"
        os.environ["redis_server_port_key"] = "6379"

        self.create_test_kafka_docker_container()
        self.kafka_producer_instance = Producer()
        self.kafka_consumer_instance = Consumer()

    def test_run(self):
        logging_to_console_and_syslog("Validating kafka_instance to be not null.")
        self.assertIsNotNone(self.kafka_producer_instance)
        logging_to_console_and_syslog("Posting messages to kafkaQ.")
        self.assertTrue(self.post_messages_to_kafka())
        logging_to_console_and_syslog("Reading messages from kafkaQ.")
        self.assertTrue(self.kafka_consumer_instance.connect_and_poll_for_new_message())
        logging_to_console_and_syslog("Validating if the consumer successfully dequeued messages.")
        self.assertEqual(self.kafka_producer_instance.get_current_job_count(),
                         self.kafka_consumer_instance.get_current_job_count())

    def post_messages_to_kafka(self):
        messages = [str(x) for x in range(1000)]
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
        #time.sleep(120)

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


if __name__ == "__main__":
    unittest.main()
