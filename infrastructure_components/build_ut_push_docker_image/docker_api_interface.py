# import infrastructure_components.dockerized_unit_test_framework.ut_framework
import os
import sys
import unittest
import subprocess
import docker
import time
from docker.types.services import Mount


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


class DockerAPIInterface(unittest.TestCase):
    def __init__(self,
                 docker_tag=None,
                 image_name=None,
                 dockerfile_directory_name=None):

        if docker_tag:
            self.docker_tag = docker_tag
        else:
            self.docker_tag = "ssriram1978"

        if image_name:
            self.image_name = image_name
        else:
            self.image_name = "unittest"

        self.docker_image_name = "{}/{}:latest".format(self.docker_tag, self.image_name)

        if dockerfile_directory_name:
            self.dirname = dockerfile_directory_name
        else:
            logging_to_console_and_syslog("Cannot proceed without knowing the path to dockerfile.")
            raise BaseException

        self.docker_instance = docker.from_env()
        self.container = None

    def create_subprocess(self, process_args):
        if not process_args or type(process_args) != list:
            return None

        completedProcess = subprocess.run(process_args,
                                          stdout=subprocess.PIPE)
        self.assertIsNotNone(completedProcess)
        self.assertIsNotNone(completedProcess.stdout)
        return completedProcess.stdout.decode('utf8')

    @staticmethod
    def find_directory_containing_package(source_package_name):
        source_directory = None
        cwd_list = os.getcwd().split('/')

        logging_to_console_and_syslog("Trying to look for {}"
                                      " in directory {}."
                                      .format(source_package_name,
                                              os.getcwd()))

        for index in range(len(cwd_list) - 1, -1, -1):
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
                logging_to_console_and_syslog("Found package {} "
                                              "in directory {}."
                                              .format(source_package_name,
                                                      output[0]))
                source_directory = output[0]
                break
        return source_directory

    def create_gzipped_directory(self, source_package_name, destination):
        # example: tar -C /home/sriramsridhar/git/briefcam_MEC_POC -czvf
        # infrastructure_components.tar.gz infrastructure_components
        source = DockerAPIInterface.find_directory_containing_package(source_package_name)
        if not source:
            return
        logging_to_console_and_syslog("Trying to find log files in source {}."
                                      .format(source))
        # clean up log files
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

        # extract the filename.
        filename = source.split('/')[-1]
        parent_directory = '/'.join(source.split('/')[:-1])
        logging_to_console_and_syslog("Going to gzip directory {}"
                                      " from parent directory {}"
                                      .format(filename,
                                              parent_directory))
        # tar.gzip the file.
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
        logging_to_console_and_syslog("Setting docker_tag={},"
                                      "image_name={}"
                                      .format(self.docker_tag,
                                              self.image_name))
        logging_to_console_and_syslog("Zipping infrastructure components "
                                      "to be added to the docker image")
        self.create_gzipped_directory("infrastructure_components",
                                      self.dirname)

        logging_to_console_and_syslog("Building docker image {}"
                                      .format(self.docker_image_name))

        docker_create_command_list = ["docker",
                                      "build",
                                      self.dirname,
                                      "-t",
                                      self.docker_image_name
                                      ]
        output = self.create_subprocess(docker_create_command_list)
        logging_to_console_and_syslog(output)
        self.assertIsNotNone(output)

        #Build a special docker image for unit test.
        #Note that this docker image has the unit test file already mentioned in the RUN command.
        if "machine_learning_workers" in self.dirname:
            logging_to_console_and_syslog("Preparing a special image for unit testing {}"
                                          .format(self.dirname))
            docker_create_command_list = ["docker",
                                          "build",
                                          self.dirname,
                                          "-f"
                                          "{}/Dockerfile.unittest".format(self.dirname),
                                          "-t",
                                          "{}/{}_unittest".format(self.docker_tag,
                                                                         self.image_name)
                                          ]
            output = self.create_subprocess(docker_create_command_list)
            logging_to_console_and_syslog(output)
            self.assertIsNotNone(output)

    def __run_docker_container(self, command=None):
        """
        bind_mount = "/usr/bin/docker:/usr/bin/docker".split()
        path_mount = []
        for path in bind_mount:
            path_list = path.split(':')
            logging_to_console_and_syslog("Appending target={} "
                                          "and source={} to mount list."
                                          .format(path_list[0],
                                                  path_list[1]))
            path_mount.append(Mount(target=path_list[0],
                                    source=path_list[1]))
        volume_mount = "/var/run/docker.sock:/var/run/docker.sock"
        """
        volume = {'/var/run/docker.sock': {'bind': '/var/run/docker.sock'},
                  '/usr/bin/docker': {'bind': '/usr/bin/docker'}}
        mount = [Mount('/usr/bin/docker', '/usr/bin/docker')]

        if command:
            logging_to_console_and_syslog("Running docker container "
                                          "{} "
                                          "with command {}"
                                          .format(self.docker_image_name,
                                                  command))

            self.container = self.docker_instance.containers.run(
                self.docker_image_name,
                # mounts=mount,
                volumes=volume,
                name=self.image_name,
                network_mode="host",
                command=command,
                detach=True)
        else:
            logging_to_console_and_syslog("Running docker container "
                                          "{} "
                                          .format(self.docker_image_name))

            self.container = self.docker_instance.containers.run(
                self.docker_image_name,
                volumes=volume,
                # mounts=mount,
                name=self.image_name,
                network_mode="host",
                detach=True)
        return self.container.short_id

    @staticmethod
    def getNetworkIp():
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.connect(('<broadcast>', 0))
        return s.getsockname()[0]

    def run_docker_container2(self, docker_image_name=None):
        if not docker_image_name:
            docker_image_name = self.docker_image_name

        completed_process = subprocess.run(["docker",
                                            "run",
                                            "-itd",
                                            "-v",
                                            "/var/run/docker.sock:/var/run/docker.sock",
                                            "-v",
                                            "/usr/bin/docker:/usr/bin/docker",
                                            "-p",
                                            "5901:5900",
                                            "--add-host",
                                            "mec-demo:10.2.40.160",
                                            "--add-host",
                                            "mec-poc:{}".format(DockerAPIInterface.getNetworkIp()),
                                            docker_image_name],
                                           stdout=subprocess.PIPE)
        cont_id = completed_process.stdout.decode('utf8')
        logging_to_console_and_syslog("cont_id={}".format(cont_id[:12]))
        self.container = self.docker_instance.containers.get(cont_id[:12])
        if self.container:
            logging_to_console_and_syslog("docker container id ={}."
                                          .format(self.container.short_id))
        else:
            logging_to_console_and_syslog("Unable to start the docker container.")
            raise Exception

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
        if result['StatusCode'] == 0:
            logging_to_console_and_syslog("The docker exited successfully.")
        else:
            logging_to_console_and_syslog("The docker exited with code {}."
                                          .format(result['StatusCode']))

    def prune_old_docker_image(self):
        docker_prune_command_list = ["docker",
                                     "container",
                                     "prune",
                                     "-f"]
        self.assertIsNotNone(self.create_subprocess(docker_prune_command_list))
        docker_prune_command_list = ["docker",
                                     "image",
                                     "prune",
                                     "-f"]
        self.assertIsNotNone(self.create_subprocess(docker_prune_command_list))

    def capture_docker_container_logs(self):
        docker_container_log_list = ["docker",
                                     "logs",
                                     self.image_name]
        logging_to_console_and_syslog(self.create_subprocess(docker_container_log_list))

    def __remove_docker_image(self):
        docker_container_remove_image_list = ["docker",
                                              "image",
                                              "rm",
                                              "-f",
                                              self.docker_image_name]
        self.assertIsNotNone(self.create_subprocess(docker_container_remove_image_list))
        # remove the infrastructure package
        parent_dirname = '/'.join(self.dirname.split('/')[:-1])
        completed_process = subprocess.run(["rm",
                                            "-f",
                                            "{}/infrastructure_components.tar.gz".format(parent_dirname)],
                                           stdout=subprocess.PIPE)

    def create_docker_container(self):
        logging_to_console_and_syslog("Prune old docker container images.")
        self.prune_old_docker_image()
        logging_to_console_and_syslog("Creating Docker image.")
        self.create_docker_image()

    def run_docker_container(self, command=None):
        logging_to_console_and_syslog("Running Docker image.")
        self.__run_docker_container(command)

    def yield_container(self):
        """
        This function reads all active docker containers and returns them back to the caller one container id at a time.
        """
        for container in self.docker_instance.containers.list():
            yield container

    def stop_docker_container_by_name(self):
        for container in self.yield_container():
            logging_to_console_and_syslog("container.name="+ container.name
                                          + "container.image=" + str(container.image)
                                          + "container.image=" + repr(container.image)
                                          + "container.status="+container.status)

            if self.docker_image_name in repr(container.image):
                container_instance = container.attach()
                logging_to_console_and_syslog("Stopping container name={}."
                                              .format(container.image))
                container.stop()
                break

    def stop_docker_container(self, container_id=None):
        try:
            if container_id is None:
                container_id = self.container.id

            # logging_to_console_and_syslog("Stopping container {}.".format(self.container.id))
            # self.docker_instance.containers.stop(self.container.id)
            logging_to_console_and_syslog("Deleting container {}.".format(container_id))
            completed_process = subprocess.run(["docker",
                                                "container",
                                                "rm",
                                                "-f",
                                                container_id],
                                               stdout=subprocess.PIPE)
            """
            logging_to_console_and_syslog("kill container {}.".format(self.container.id))
            self.docker_instance.containers.kill(self.container.id)
            logging_to_console_and_syslog("remove container {}.".format(self.container.id))
            self.docker_instance.containers.remove(self.container.id)
            """

        except:
            return None

    def remove_docker_image(self):
        logging_to_console_and_syslog("Removing docker image.")
        self.__remove_docker_image()

    def deploy(self, dockerfile_path):
        logging_to_console_and_syslog("Deploying docker image"
                                      " found in directory {}."
                                      .format(dockerfile_path))
        """
        Push the docker image to Github.
        Credentials are pre-stored on the localhost.
        :return:
        """
        docker_push_command_list = ["docker",
                                    "image",
                                    "push",
                                    "{}/{}".format(self.docker_tag,
                                                   self.image_name)
                                    ]
        self.assertIsNotNone(self.create_subprocess(docker_push_command_list))
