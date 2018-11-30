#import infrastructure_components.dockerized_unit_test_framework.ut_framework
import os
import sys
import unittest
import subprocess
import docker


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


class BuildAndTestThisDocker(unittest.TestCase):
    DOCKER_TAG = None
    DOCKER_IMAGE_NAME = None

    def setUp(self):
        BuildAndTestThisDocker.DOCKER_TAG = os.getenv("DOCKER_TAG",
                                                      default=None)

        BuildAndTestThisDocker.DOCKER_IMAGE_NAME = os.getenv("DOCKER_IMAGE_NAME",
                                                             default=None)
        self.docker_instance = docker.from_env()
        self.container = None
        self.dirname = '/'.join(os.path.dirname(os.path.realpath(__file__)).split('/')[:-1])

    def create_subprocess(self, process_args):
        if not process_args or type(process_args) != list:
            return None

        completedProcess = subprocess.run(process_args,
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)
        return completedProcess.stdout.decode('utf8')

    def find_directory_containing_package(self,source_package_name):
        source_directory = None
        cwd_list = os.getcwd().split('/')

        logging_to_console_and_syslog("Trying to look for {}"
                                      " in directory {}."
                                      .format(source_package_name,
                                              os.getcwd()))

        for index in range(len(cwd_list)-1,-1,-1):
            current_directory = '/'.join(cwd_list[:index])
            logging_to_console_and_syslog("Trying to look for {}"
                                          " in directory {}."
                                          .format(source_package_name,
                                                  current_directory))
            completed_process = subprocess.run(["find",
                                            current_directory,
                                            "-name",
                                            source_package_name],
                                           stdout=subprocess.PIPE)
            output = completed_process.stdout.decode('utf8').split('\n')
            if source_package_name in output[0]:
                logging_to_console_and_syslog("Found pakage {}"
                                              "in directory {}."
                                              .format(source_package_name,
                                                      output[0]))
                source_directory = output[0]
                break
        return source_directory

    def create_gzipped_directory(self, source_package_name, destination):
        #example: tar -C /home/sriramsridhar/git/briefcam_MEC_POC -czvf infrastructure_components.tar.gz infrastructure_components
        source = self.find_directory_containing_package(source_package_name)
        if not source:
            return
        logging_to_console_and_syslog("Trying to find log files in source {}."
                                      .format(source))
        #clean up log files
        completed_process = subprocess.run(["find",
                                             source,
                                             "-name",
                                             "*.log"],
                                            stdout=subprocess.PIPE)

        for filename in completed_process.stdout.decode('utf8').split('\n'):
            if '.log' in filename:
                logging_to_console_and_syslog("Deleting log file {}."
                                          .format(filename))
                subprocess.run(["rm", "-f", filename], stdout=subprocess.PIPE)

        #extract the filename.
        filename = source.split('/')[-1]
        parent_directory = '/'.join(source.split('/')[:-1])
        logging_to_console_and_syslog("Going to gzip directory {}"
                                      " from parent directory {}"
                                      .format(filename, parent_directory))
        #tar.gzip the file.
        gzipped_file = "{}/{}.tar.gz".format(destination, filename)
        completed_process = subprocess.run(["tar",
                                            "-C",
                                            parent_directory,
                                            "-czvf",
                                            gzipped_file,
                                            filename],
                                           stdout=subprocess.PIPE)

        logging_to_console_and_syslog("successfully gzipped "
                                      "source directory {}, "
                                      " and stored it in the dest directory {}"
                                      " as {}"
                                      .format(source,
                                              destination,
                                              gzipped_file))
    def create_docker_image(self):
        if not BuildAndTestThisDocker.DOCKER_TAG or \
                not BuildAndTestThisDocker.DOCKER_IMAGE_NAME:
            BuildAndTestThisDocker.DOCKER_TAG = 'ssriram1978'
            BuildAndTestThisDocker.DOCKER_IMAGE_NAME = "unit_test"

        logging_to_console_and_syslog("Setting DOCKER_TAG={},"
                                      "DOCKER_IMAGE_NAME={}"
                                      .format(BuildAndTestThisDocker.DOCKER_TAG,
                                              BuildAndTestThisDocker.DOCKER_IMAGE_NAME))
        logging_to_console_and_syslog("Zipping infrastructure components to be added to the docker image")
        self.create_gzipped_directory("infrastructure_components", self.dirname)

        docker_create_command_list = ["docker",
                                      "build",
                                      self.dirname,
                                      "-t",
                                      "{}/{}:latest".format(BuildAndTestThisDocker.DOCKER_TAG,
                                                            BuildAndTestThisDocker.DOCKER_IMAGE_NAME)]
        output = self.create_subprocess(docker_create_command_list)
        logging_to_console_and_syslog(output)
        self.assertIsNotNone(output)

    def run_docker_container(self):
        bind_mount = "/var/run/docker.sock:/var/run/docker.sock /usr/bin/docker:/usr/bin/docker".split()
        self.container = self.docker_instance.containers.run(
            "{}/{}:latest".format(BuildAndTestThisDocker.DOCKER_TAG,
                                  BuildAndTestThisDocker.DOCKER_IMAGE_NAME),
            volumes=bind_mount,
            name=BuildAndTestThisDocker.DOCKER_IMAGE_NAME,
            network_mode="host",
            command="python3 unittest/test_job_dispatcher.py",
            detach=True)
        self.assertIsNotNone(self.container.short_id)

    def wait_for_docker_container_completion(self):
        result = None
        try:
            container = self.docker_instance.containers.get(self.container.short_id)
            if container:
                result = container.wait()
        except:
            return None

        logging_to_console_and_syslog("Capturing container logs.")
        self.capture_docker_container_logs()
        logging_to_console_and_syslog("Result returned {}.".format(result))
        self.assertEqual(result['StatusCode'], 0)

    def prune_old_docker_image(self):
        docker_prune_command_list = ["docker",
                                     "container",
                                     "prune",
                                     "-f"]
        self.assertIsNotNone(self.create_subprocess(docker_prune_command_list))

    def capture_docker_container_logs(self):
        docker_container_log_list = ["docker",
                                     "logs",
                                     BuildAndTestThisDocker.DOCKER_IMAGE_NAME]
        logging_to_console_and_syslog(self.create_subprocess(docker_container_log_list))

    def remove_docker_image(self):
        docker_container_remove_image_list = ["docker",
                                              "image",
                                              "rm",
                                              "-f",
                                              "{}/{}".format(BuildAndTestThisDocker.DOCKER_TAG,
                                                             BuildAndTestThisDocker.DOCKER_IMAGE_NAME)]
        self.assertIsNotNone(self.create_subprocess(docker_container_remove_image_list))

    def test_docker_container(self):
        logging_to_console_and_syslog("Creating Docker image.")
        self.create_docker_image()
        logging_to_console_and_syslog("Prune old docker container images.")
        self.prune_old_docker_image()
        logging_to_console_and_syslog("Running Docker image.")
        self.run_docker_container()
        logging_to_console_and_syslog("Waiting for the Docker image to complete.")
        self.wait_for_docker_container_completion()
        logging_to_console_and_syslog("Removing docker image.")
        self.remove_docker_image()
        logging_to_console_and_syslog("Completed unit testing.")

    def tearDown(self):
        #remove the infrastructure package
        parent_dirname = '/'.join(self.dirname.split('/')[:-1])
        completed_process = subprocess.run(["rm",
                                            "-f",
                                            "{}/infrastructure_components.tar.gz".format(parent_dirname)],
                                           stdout=subprocess.PIPE)


#suite = unittest.TestSuite()
#suite.addTest(unittest.makeSuite(BuildAndTestThisDocker))
#runner = unittest.TextTestRunner()
#print(runner.run(suite))
