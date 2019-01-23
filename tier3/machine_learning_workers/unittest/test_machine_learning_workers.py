import os
import time
import sys
import traceback
import unittest
import subprocess
import threading


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
from infrastructure_components.redis_client.redis_interface import RedisInterface
from infrastructure_components.producer_consumer.producer_consumer import ProducerConsumerAPI
from infrastructure_components.data_parser.data_parser_interface import DataParserInterface
from tier3.machine_learning_workers.machine_learning_worker import MachineLearningWorker
from infrastructure_components.build_ut_push_docker_image.docker_api_interface import DockerAPIInterface

class TestMachineLearningWorkers(unittest.TestCase):

    max_number_of_jobs = 100
    directory_name = 'test_files'

    def setUp(self):
        os.environ["broker_name_key"] = "{}:9094".format("mec-poc")
        os.environ["topic_key"] = "video-file-name"
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["total_job_enqueued_count_redis_name_key"] = "enqueue"
        os.environ["total_job_dequeued_count_redis_name_key"] = "dequeue"
        os.environ["redis_server_hostname_key"] = "mec-poc"
        os.environ["redis_server_port_key"] = "6379"
        os.environ["producer_consumer_queue_type_key"] = ProducerConsumerAPI.kafkaMsgQType
        os.environ["data_parser_type_key"] = DataParserInterface.TensorFlow
        self.dirname = os.path.dirname(os.path.realpath(__file__))
        current_file_path_list = os.path.realpath(__file__).split('/')
        data_path_directory = '/'.join(current_file_path_list[:-1])
        os.environ["data_file_path_key"] = "{}/{}".format(data_path_directory,
                                                           TestMachineLearningWorkers.directory_name)
        self.create_test_docker_container()
        self.machine_learning_worker_thread = None
        self.producer_instance = None

    @staticmethod
    def start_machine_learning_workers():
        logging_to_console_and_syslog("Starting {}".format(threading.current_thread().getName()))
        t = threading.currentThread()
        worker = MachineLearningWorker()
        while getattr(t, "do_run", True):
            #logging_to_console_and_syslog("***dequeue_and_process_jobs***")
            worker.dequeue_and_process_jobs()
        logging_to_console_and_syslog("Consumer {}: Exiting"
                                      .format(threading.current_thread().getName()))

    def create_machine_learning_worker_thread(self):
        self.machine_learning_worker_thread = \
            threading.Thread(name="{}{}".format("thread", 1),
            target=TestMachineLearningWorkers.start_machine_learning_workers
            )
        self.machine_learning_worker_thread.do_run = True
        self.machine_learning_worker_thread.name = "{}_{}".format("test_machine_learning_workers", 1)
        self.machine_learning_worker_thread.start()

    def post_messages(self):
        self.redis_instance = RedisInterface("Producer")
        messages = [str(x) for x in range(TestMachineLearningWorkers.max_number_of_jobs)]
        for message in messages:
            self.producer_instance.enqueue(message)
            event = "Producer: Successfully posted a message = {} into msgQ.".format(message)
            self.redis_instance.write_an_event_in_redis_db(event)
            self.redis_instance.increment_enqueue_count()
        return True
    
    def create_producer_and_produce_jobs(self, msgq_type):
        self.producer_instance = ProducerConsumerAPI(is_producer=True,
                                                     thread_identifier="Producer",
                                                     type_of_messaging_queue=msgq_type)
        logging_to_console_and_syslog("Posting messages.")
        self.assertTrue(self.post_messages())
        
    def validate_machine_learning_workers(self):
        self.create_machine_learning_worker_thread()
        time.sleep(30)
        self.create_producer_and_produce_jobs(ProducerConsumerAPI.kafkaMsgQType)
        time.sleep(5)
        logging_to_console_and_syslog("Validating if the machine learning workers"
                                      " successfully enqueued the messages.")
        redis_instance = RedisInterface("Consumer")
        self.assertEqual(redis_instance.get_current_dequeue_count().decode('utf8'),
                         str(TestMachineLearningWorkers.max_number_of_jobs))
        logging_to_console_and_syslog("dequeue_count={},max_number_of_jobs={}"
                                      .format(redis_instance.get_current_dequeue_count(),
                                              TestMachineLearningWorkers.max_number_of_jobs))

    def test_run(self):
        logging_to_console_and_syslog("Validating **************** Machine Learning workers *****************.")
        self.validate_machine_learning_workers()

    def create_test_docker_container(self):
        completedProcess = subprocess.run(["sudo",
                                           "docker-compose",
                                           "-f",
                                           "{}/docker-compose_wurstmeister_kafka.yml".format(self.dirname),
                                           "up",
                                           "-d"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def delete_test_docker_container(self):
        completedProcess = subprocess.run(["sudo",
                                           "docker-compose",
                                           "-f",
                                           "{}/docker-compose_wurstmeister_kafka.yml".format(self.dirname),
                                           "down"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def tearDown(self):
        self.delete_test_docker_container()
        subprocess.run(['rm', '-rf', TestMachineLearningWorkers.directory_name], stdout=subprocess.PIPE)
        self.machine_learning_worker_thread.do_run = False
        time.sleep(1)
        logging_to_console_and_syslog("Trying to join thread.")
        self.machine_learning_worker_thread.join(1.0)
        time.sleep(1)
        if self.machine_learning_worker_thread.isAlive():
            try:
                logging_to_console_and_syslog("Trying to __stop thread.")
                self.machine_learning_worker_thread._stop()
            except:
                logging_to_console_and_syslog("Caught an exception while stopping thread.")
                docker_api_interface_instance = DockerAPIInterface(image_name=self.dirname.split('/')[-2],
                                                                   dockerfile_directory_name=self.dirname)
                docker_api_interface_instance.stop_docker_container_by_name()


if __name__ == "__main__":
    # To avoid the end of execution traceback adding exit=False
    unittest.main(exit=False)
