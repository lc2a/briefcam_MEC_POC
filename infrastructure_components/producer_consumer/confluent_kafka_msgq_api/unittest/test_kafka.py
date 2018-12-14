import os
import time
import sys
import traceback
import unittest
import subprocess
import threading

#sys.path.append("..")  # Adds higher directory to python modules path.

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
from infrastructure_components.producer_consumer.confluent_kafka_msgq_api.confluent_kafka_msgq_api import ConfluentKafkaMsgQAPI
from infrastructure_components.redis_client.redis_interface import RedisInterface
from infrastructure_components.build_ut_push_docker_image.docker_api_interface import DockerAPIInterface


class TestProducerConsumer(unittest.TestCase):
    def setUp(self):
        os.environ["broker_name_key"] = "localhost:9092"
        os.environ["topic_key"] = "video-file-name"
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["total_job_enqueued_count_redis_name_key"] = "enqueue"
        os.environ["total_job_dequeued_count_redis_name_key"] = "dequeue"
        os.environ["redis_server_hostname_key"] = "localhost"
        os.environ["redis_server_port_key"] = "6379"
        self.dirname = os.path.dirname(os.path.realpath(__file__))
        self.max_consumer_threads = 10
        self.create_test_docker_container()
        self.producer_instance = None
        self.consumer_threads = None
        self.redis_instance = None

    @staticmethod
    def run_consumer_instance():
        logging_to_console_and_syslog("Starting {}".format(threading.current_thread().getName()))
        t = threading.currentThread()
        redis_instance = RedisInterface(threading.current_thread().getName())

        while getattr(t, "do_run", True):
            consumer_instance = ConfluentKafkaMsgQAPI(is_consumer=True,
                                                      thread_identifier=threading.current_thread().getName(),
                                                      perform_subscription=True)
            message = consumer_instance.dequeue()
            if message:
                logging_to_console_and_syslog("Consumer {}: Dequeued Message = {}"
                                              .format(threading.current_thread().getName(),
                                                      message))
                redis_instance.increment_dequeue_count()
                redis_instance.write_an_event_in_redis_db("Consumer {}: Dequeued Message = {}"
                                                          .format(threading.current_thread().getName(),
                                                                  message))
            consumer_instance.cleanup()
            time.sleep(10)

        logging_to_console_and_syslog("Consumer {}: Exiting"
                                      .format(threading.current_thread().getName()))

    def create_consumer_threads(self):
        self.consumer_threads = [0] * self.max_consumer_threads
        for index in range(self.max_consumer_threads):
            self.consumer_threads[index] = threading.Thread(name="{}{}".format("thread", index),
                                                            target=TestProducerConsumer.run_consumer_instance)
            self.consumer_threads[index].do_run = True
            self.consumer_threads[index].name = "{}_{}".format("consumer",index)
            self.consumer_threads[index].start()

    def create_consumers(self):
        self.create_consumer_threads()
        logging_to_console_and_syslog("Validating consumer threads to be not null.")
        for index in range(self.max_consumer_threads):
            self.assertIsNotNone(self.consumer_threads[index])

    def create_producer_and_produce_jobs(self):
        self.producer_instance = ConfluentKafkaMsgQAPI(is_producer=True,
                                                       thread_identifier="Producer")
        logging_to_console_and_syslog("Posting messages.")
        self.assertTrue(self.post_messages())

    def test_run(self):
        logging_to_console_and_syslog("Creating producer instance and producing jobs.")
        self.create_producer_and_produce_jobs()
        logging_to_console_and_syslog("Validating producer instance to be not null.")
        self.assertIsNotNone(self.producer_instance)
        self.create_consumers()
        time.sleep(120)
        logging_to_console_and_syslog("Validating if the consumer successfully dequeued messages.")
        redis_instance = RedisInterface(threading.current_thread().getName())
        self.assertEqual(redis_instance.get_current_enqueue_count(),
                         redis_instance.get_current_dequeue_count())
        logging_to_console_and_syslog("enqueue_count={},dequeue_count={}"
                                      .format(redis_instance.get_current_enqueue_count(),
                                              redis_instance.get_current_dequeue_count()))

    def post_messages(self):
        self.redis_instance = RedisInterface("Producer")
        messages = [str(x) for x in range(100)]
        for message in messages:
            self.producer_instance.enqueue(message)
            event = "Producer: Successfully posted a message = {} into Kafka.".format(message)
            self.redis_instance.write_an_event_in_redis_db(event)
            self.redis_instance.increment_enqueue_count()
        return True

    def create_test_docker_container(self):
        completedProcess = subprocess.run(["docker-compose",
                                           "-f",
                                           "{}/docker-compose_confluent_kafka.yml".format(self.dirname),
                                           "up",
                                           "-d"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)
        # time.sleep(120)

    def delete_test_docker_container(self):
        completedProcess = subprocess.run(["docker-compose",
                                           "-f",
                                           "{}/docker-compose_confluent_kafka.yml".format(self.dirname),
                                           "down"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def tearDown(self):
        self.producer_instance.cleanup()
        self.delete_test_docker_container()
        time.sleep(5)
        for index in range(self.max_consumer_threads):
            self.consumer_threads[index].do_run = False
            time.sleep(1)
            logging_to_console_and_syslog("Trying to join thread {}."
                                          .format(self.consumer_threads[index].getName()))
            self.consumer_threads[index].join(1.0)
            if self.consumer_threads[index].is_alive():
                try:
                    logging_to_console_and_syslog("Trying to __stop thread {}."
                                                  .format(self.consumer_threads[index].getName()))
                    self.consumer_threads[index]._stop()

                except:
                    logging_to_console_and_syslog("Caught an exception while stopping thread {}"
                                                  .format(self.consumer_threads[index].getName()))
                    docker_image_name = self.dirname.split('/')[-2]
                    logging_to_console_and_syslog("Trying to stop docker image{}."
                                                  .format(docker_image_name))
                    try:
                        docker_api_interface_instance = DockerAPIInterface(image_name=docker_image_name,
                                                                           dockerfile_directory_name=self.dirname)
                        docker_api_interface_instance.stop_docker_container_by_name()
                    except:
                        logging_to_console_and_syslog("Caught an exception while stopping {}"
                                                      .format(docker_image_name))
                        print("Exception in user code:")
                        print("-" * 60)
                        traceback.print_exc(file=sys.stdout)
                        print("-" * 60)


if __name__ == "__main__":
    unittest.main()