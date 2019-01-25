import time
import os
import sys
import traceback


def import_all_packages():
    realpath = os.path.realpath(__file__)
    # print("os.path.realpath({})={}".format(__file__,realpath))
    dirname = os.path.dirname(realpath)
    # print("os.path.dirname({})={}".format(realpath,dirname))
    dirname_list = dirname.split('/')
    # print(dirname_list)
    for index in range(len(dirname_list)):
        module_path = '/'.join(dirname_list[:index])
        # print("module_path={}".format(module_path))
        try:
            sys.path.append(module_path)
        except:
            # print("Invalid module path {}".format(module_path))
            pass


import_all_packages()

from infrastructure_components.log.log_file import logging_to_console_and_syslog
from infrastructure_components.redis_client.redis_interface import RedisInterface
from infrastructure_components.docker.docker_service import DockerService


class AutoScaler:
    def __init__(self):
        self.docker_instance = None
        self.min_threshold = -1
        self.max_threshold = -1
        self.auto_scale_service_name = None
        self.auto_scale_time_interval = 10
        self.scale_down_count = 0
        self.scale_down_count_max_threshold = 0
        self.redis_instance = RedisInterface("AutoScaler")
        self.__load_environment_variables()
        self.__perform_auto_scaling()

    def __load_environment_variables(self):
        while self.min_threshold is -1 or \
                self.max_threshold is -1 or \
                not self.auto_scale_service_name:
            time.sleep(1)
            self.min_threshold = int(os.getenv("min_threshold_key", default=-1))
            self.max_threshold = int(os.getenv("max_threshold_key", default=-1))
            self.scale_down_count_max_threshold = int(os.getenv("scale_down_count_max_threshold_key", default=60))
            self.auto_scale_time_interval = int(os.getenv("auto_scale_time_interval_key", default=10))
            self.auto_scale_service_name = os.getenv("auto_scale_service_name_key", default=None)

        logging_to_console_and_syslog(("min_threshold={}".format(self.min_threshold)))
        logging_to_console_and_syslog(("max_threshold={}".format(self.max_threshold)))
        logging_to_console_and_syslog(("auto_scale_service_name={}".format(self.auto_scale_service_name)))
        logging_to_console_and_syslog(("auto_scale_time_interval={}".format(self.auto_scale_time_interval)))

    def __perform_scale_down_operation(self):
        current_number_of_docker_instances = \
            self.docker_instance.get_current_number_of_containers_per_service()
        if current_number_of_docker_instances == self.max_threshold and \
                current_number_of_docker_instances - 30 >= self.min_threshold:
            self.docker_instance.scale(current_number_of_docker_instances - 30)
        elif current_number_of_docker_instances <= self.max_threshold // 2 and \
                current_number_of_docker_instances - 20 >= self.min_threshold:
            self.docker_instance.scale(current_number_of_docker_instances - 20)
        elif current_number_of_docker_instances <= self.max_threshold // 4 and \
                current_number_of_docker_instances - 10 >= self.min_threshold:
            self.docker_instance.scale(current_number_of_docker_instances - 10)
        elif current_number_of_docker_instances - 1 >= self.min_threshold:
            self.docker_instance.scale(current_number_of_docker_instances - 1)

    def __perform_scale_up_operation(self, jobs_in_pipe):
        current_number_of_docker_instances = \
            self.docker_instance.get_current_number_of_containers_per_service()
        if 0 < jobs_in_pipe <= 10 and \
                current_number_of_docker_instances + 1 < self.max_threshold:
            self.docker_instance.scale(current_number_of_docker_instances + 1)
        elif 11 < jobs_in_pipe <= 50:
            if current_number_of_docker_instances + 10 < self.max_threshold:
                self.docker_instance.scale(current_number_of_docker_instances + 10)
            else:
                self.docker_instance.scale(current_number_of_docker_instances +
                                           self.max_threshold-current_number_of_docker_instances)
        elif 50 < jobs_in_pipe <= 100:
            if current_number_of_docker_instances + 20 < self.max_threshold:
                self.docker_instance.scale(current_number_of_docker_instances + 20)
            else:
                self.docker_instance.scale(current_number_of_docker_instances +
                                           self.max_threshold - current_number_of_docker_instances)
        else:
            if current_number_of_docker_instances + 30 < self.max_threshold:
                self.docker_instance.scale(current_number_of_docker_instances + 30)
            else:
                self.docker_instance.scale(current_number_of_docker_instances +
                                           self.max_threshold - current_number_of_docker_instances)

    def __perform_auto_scaling(self):
        """
        Wake up every pre-specified time interval and do the following:
        Read the current total_job_done_count from Redis.
        Read the current total_job_to_be_done_count from Redis.
        Compute the difference between total_job_to_be_done_count and total_job_done_count.
        This gives the total number of jobs in the pipe.
        Now, count the current number of consumer instances (containers) for the specified docker service.
        if the current count of the number of instances is greater than max_threshold, then,
            return False
        if the total number of jobs in the pipe is 0:
            if current count of the number of instance is equal to max_threshold:
                scale down by 30
            elif current count of the number of instance is above max_threshold//2:
                scale down by 20
            elif current count of the number of instance is above max_threshold//4:
                scale down by 10
            else:
                scale down by 1
        elif the total number of jobs in the pipe is between 1-10:
            scale up by 1
        elif total number of jobs in the pipe is between 11-50:
            scale up by 10
        elif total number of jobs in the pipe is between 51-100:
            scale up by 20
        elif total number of jobs in the pipe is between 101-200:
            scale up by 30
        :return:
        """
        self.docker_instance = DockerService(self.auto_scale_service_name)
        while True:
            time.sleep(self.auto_scale_time_interval)
            current_job_to_be_done_count = int(self.redis_instance.get_current_enqueue_count())
            current_job_done_count = int(self.redis_instance.get_current_dequeue_count())
            jobs_in_pipe = current_job_to_be_done_count - current_job_done_count
            logging_to_console_and_syslog("current_job_to_be_done_count={},"
                                          "current_job_done_count={},"
                                          "jobs_in_pipe={}."
                                          .format(current_job_to_be_done_count,
                                                  current_job_done_count,
                                                  jobs_in_pipe))
            if jobs_in_pipe <= 0:
                if self.scale_down_count == self.scale_down_count_max_threshold:
                    logging_to_console_and_syslog("Performing scale down operation.")
                    self.__perform_scale_down_operation()
                    self.scale_down_count = 0
                else:
                    self.scale_down_count += 1
                    logging_to_console_and_syslog("Bumping up self.scale_down_count to {}."
                                                  .format(self.scale_down_count))
            else:
                logging_to_console_and_syslog("Performing scale up operation.")
                self.__perform_scale_up_operation(jobs_in_pipe)
                self.scale_down_count = 0

    def cleanup(self):
        pass


if __name__ == "__main__":
    try:
        auto_scaler = AutoScaler()
    except:
        logging_to_console_and_syslog("Exception occurred.{}".format(sys.exc_info()))
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
