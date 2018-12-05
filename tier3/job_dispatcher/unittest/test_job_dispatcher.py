import os
import time
import sys
import traceback
import unittest
import subprocess
import threading


# sys.path.append("..")  # Adds higher directory to python modules path.

def import_all_packages():
    realpath = os.path.realpath(__file__)
    # print("os.path.realpath({})={}".format(__file__,realpath))
    dirname = os.path.dirname(realpath)
    print("os.path.dirname({})={}".format(realpath, dirname))
    dirname_list = dirname.split('/')
    # print(dirname_list)
    for index in range(len(dirname_list)):
        module_path = '/'.join(dirname_list[:index])
        print("Appending module_path={} to sys.path".format(module_path))
        try:
            sys.path.append(module_path)
        except:
            # print("Invalid module path {}".format(module_path))
            pass


import_all_packages()
from infrastructure_components.log.log_file import logging_to_console_and_syslog
from infrastructure_components.redis_client.redis_interface import RedisInterface
from tier3.job_dispatcher.job_dispatcher import DirectoryWatch


class TestJobDispatcher(unittest.TestCase):
    max_number_of_jobs = 100
    directory_name = 'test_files'

    def setUp(self):
        os.environ["broker_name_key"] = "localhost:9094"
        os.environ["topic_key"] = "video-file-name"
        os.environ["redis_log_keyname_key"] = "briefcam"
        os.environ["total_job_enqueued_count_redis_name_key"] = "enqueue"
        os.environ["total_job_dequeued_count_redis_name_key"] = "dequeue"
        os.environ["redis_server_hostname_key"] = "localhost"
        os.environ["redis_server_port_key"] = "6379"
        self.dirname = os.path.dirname(os.path.realpath(__file__))
        current_file_path_list = os.path.realpath(__file__).split('/')
        video_path_directory = '/'.join(current_file_path_list[:-1])
        os.environ["video_file_path_key"] = "{}/{}".format(video_path_directory,
                                                           TestJobDispatcher.directory_name)
        self.create_test_docker_container()
        self.producer_thread = None

    @staticmethod
    def create_new_files():
        dirname = "{}/{}".format(os.path.dirname(os.path.realpath(__file__)),
                                 TestJobDispatcher.directory_name)

        producer_instance = DirectoryWatch()
        logging_to_console_and_syslog("Creating new files.")
        subprocess.run(['mkdir',
                        dirname],
                       stdout=subprocess.PIPE)
        for index in range(TestJobDispatcher.max_number_of_jobs):
            fh = open("{}/ss_{}.txt".format(dirname,
                                            index),
                      "w")
            fh.write("Hello")
            fh.close()
        producer_instance.watch_a_directory()

    def create_producer_thread(self):
        self.producer_thread = threading.Thread(name="{}{}".format("thread", 1),
                                                target=TestJobDispatcher.create_new_files
                                                )
        self.producer_thread.do_run = True
        self.producer_thread.name = "{}_{}".format("consumer", 1)
        self.producer_thread.start()

    def perform_enqueue_dequeue(self):
        logging_to_console_and_syslog("Validating producer instance to be not null.")
        self.create_producer_thread()
        time.sleep(30)
        logging_to_console_and_syslog("Validating if the Producer successfully enqueued the messages.")
        redis_instance = RedisInterface("Producer")
        self.assertEqual(redis_instance.get_current_enqueue_count().decode('utf8'),
                         str(TestJobDispatcher.max_number_of_jobs))
        logging_to_console_and_syslog("enqueue_count={},max_number_of_jobs={}"
                                      .format(redis_instance.get_current_enqueue_count(),
                                              TestJobDispatcher.max_number_of_jobs))

    def test_run(self):
        logging_to_console_and_syslog("Validating **************** Job Dispatcher*****************.")
        self.perform_enqueue_dequeue()

    def create_test_docker_container(self):
        completedProcess = subprocess.run(["docker-compose",
                                           "-f",
                                           "{}/docker-compose_wurstmeister_kafka.yml".format(self.dirname),
                                           "up",
                                           "-d"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def delete_test_docker_container(self):
        completedProcess = subprocess.run(["docker-compose",
                                           "-f",
                                           "{}/docker-compose_wurstmeister_kafka.yml".format(self.dirname),
                                           "down"],
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)

    def tearDown(self):
        self.delete_test_docker_container()
        subprocess.run(['rm', '-rf', "{}/{}".format(self.dirname,
                                                    TestJobDispatcher.directory_name)],
                       stdout=subprocess.PIPE)
        time.sleep(5)
        self.producer_thread.join(1.0)


if __name__ == "__main__":
    try:
        unittest.main()
    except:
        logging_to_console_and_syslog("Exception occurred." + sys.exc_info()[0])
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)

