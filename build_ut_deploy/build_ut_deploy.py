import os
import sys
import subprocess
import traceback
import time
import threading

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
from infrastructure_components.build_ut_push_docker_image.build_ut_push_docker_image import DockerBuildUTPublish
from infrastructure_components.build_ut_push_docker_image.docker_api_interface import DockerAPIInterface


class DockerBuildUTDeploy:
    dockerfile_identifier = 'Dockerfile'
    unittest_identifier = 'test*.py'
    deployment_file = "docker-compose-confluent-kafka.yml"
    stack_name = "briefcam"

    def __init__(self):
        self.dockerfile_paths = []
        self.dirname = '/'.join(os.path.dirname(os.path.realpath(__file__)).split('/')[:-1])
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/tier3/auto_scaler'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/tier3/job_dispatcher'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/tier2/rtsp_recorder'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/tier2/front_end'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/tier2'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/tier3'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/redis_client'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/couchdb_client'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/producer_consumer'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/data_parser'
        #self.dirname = '/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/open_rtsp_api_handler'
        self.docker_instance = None
        self.consumer_threads = []
        self.delete_all_tar_gz_files()

    @staticmethod
    def validate_successful_completion(package_name, ut_file_path):
        logging_to_console_and_syslog("Starting {},"
                                      "package_name = {}"
                                      " ut_file_path = {}"
                                      .format(threading.current_thread().getName(),
                                              package_name,
                                              ut_file_path))
        time.sleep(180)
        docker_api_interface_instance = DockerAPIInterface(image_name=package_name,
                                                           dockerfile_directory_name=ut_file_path)
        docker_api_interface_instance.capture_docker_container_logs()
        docker_api_interface_instance.stop_docker_container_by_name()
        logging_to_console_and_syslog("Exiting {}".format(threading.current_thread().getName()))

    def delete_all_tar_gz_files(self, dirname=None):
        logging_to_console_and_syslog("Trying to delete all *.tar.gz files from {}"
                                      " directory."
                                      .format(self.dirname))
        if not dirname:
            dirname = self.dirname

        completed_process = subprocess.run(["find",
                                            dirname,
                                            "-name",
                                            "*.tar.gz",
                                            "-type",
                                            "f",
                                            "-delete"],
                                           stdout=subprocess.PIPE)
        logging_to_console_and_syslog(completed_process.stdout.decode('utf8'))

    def find_all_dockerfile_paths(self):
        logging_to_console_and_syslog("Trying to look for {}"
                                      " in directory {}."
                                      .format(DockerBuildUTDeploy.dockerfile_identifier,
                                              self.dirname))

        completed_process = subprocess.run(["find",
                                            self.dirname,
                                            "-name",
                                            DockerBuildUTDeploy.dockerfile_identifier],
                                           stdout=subprocess.PIPE)

        self.dockerfile_paths = completed_process.stdout.decode('utf8').split('\n')

    def build(self, dockerfile_path):
        logging_to_console_and_syslog("Building docker image"
                                      " found in directory {}."
                                      .format(dockerfile_path))

        self.docker_instance = DockerBuildUTPublish(dockerfile_path=dockerfile_path)
        self.docker_instance.create_docker_container()

    def push_docker_container_to_registry(self):
        self.docker_instance.push()

    def perform_unittest(self, dockerfile_path):
        logging_to_console_and_syslog("Performing Unittest of docker image"
                                      " found in directory {}."
                                      .format(dockerfile_path))
        package_name = dockerfile_path.split('/')[-1]
        completed_process = subprocess.run(["find",
                                            dockerfile_path,
                                            "-name",
                                            DockerBuildUTDeploy.unittest_identifier],
                                           stdout=subprocess.PIPE)

        unit_test_file_paths = completed_process.stdout.decode('utf8').split('\n')
        for unit_test_file_path in unit_test_file_paths:
            if package_name in unit_test_file_path:
                ut_path_list = unit_test_file_path.split('/')
                found_index = ut_path_list.index(package_name)
                if found_index:
                    ut_file_path = '/'.join(ut_path_list[found_index+1:])
                    logging_to_console_and_syslog("Running unit test file {}."
                                                  .format(ut_file_path))

                    thread_id = threading.Thread(name="{}_{}".format("thread", package_name),
                                                 target=DockerBuildUTDeploy.validate_successful_completion,
                                                 kwargs=dict(package_name=package_name,
                                                             ut_file_path=ut_file_path))
                    thread_id.start()
                    self.consumer_threads.append(thread_id)

                    if package_name == "test_data_parser" or \
                       package_name == "machine_learning_workers":
                        logging_to_console_and_syslog("Invoking a special run container API for {}"
                                                      .format(package_name))
                        self.docker_instance.run_docker_container2("ssriram1978/{}_unittest:latest"
                                                                   .format(package_name))
                    else:
                        self.docker_instance.run_docker_container("python3 {}"
                                                                  .format(ut_file_path))
                    self.docker_instance.wait_for_docker_container_completion()
                    logging_to_console_and_syslog("Stopping docker instance.")
                    self.docker_instance.stop_docker_container()

    def deploy(self):
        dirname = '/'.join(os.path.dirname(os.path.realpath(__file__)).split('/')[:-1])
        deployment_file = dirname + "/" + DockerBuildUTDeploy.deployment_file
        logging_to_console_and_syslog("Deploying {}."
                                      .format(deployment_file))
        docker_instance = DockerBuildUTPublish(dockerfile_path='/')
        docker_instance.deploy(deployment_file,
                                    DockerBuildUTDeploy.stack_name)

    def perform_build_ut_deploy(self) -> object:
        self.find_all_dockerfile_paths()
        for dockerfile_path in self.dockerfile_paths:
            if DockerBuildUTDeploy.dockerfile_identifier in dockerfile_path:
                path = '/'.join(dockerfile_path.split('/')[:-1])
                self.build(path)
                self.perform_unittest(path)
                self.push_docker_container_to_registry()
                self.delete_all_tar_gz_files(path)
        self.deploy()


if __name__ == "__main__":
    try:
        docker_build_push_image = DockerBuildUTDeploy()
        docker_build_push_image.perform_build_ut_deploy()
    except:
        logging_to_console_and_syslog("Unhandled exception {}.".format(sys.exc_info()[0]))
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
