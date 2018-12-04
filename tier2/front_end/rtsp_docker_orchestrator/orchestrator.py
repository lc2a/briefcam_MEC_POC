import time
import os
import sys
import traceback
from sys import path
import logging
import docker


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

#sys.path.append("..")  # Adds higher directory to python modules path.
from infrastructure_components.log.log_file import logging_to_console_and_syslog


class RTSPDockerOrchestrator:
    """
    This class does the following:
    1. It is a docker container orchestrator.
    2. It follows commands dictated by the master. (frontend).
    """

    def __init__(self):
        logging_to_console_and_syslog("Initializing RTSPDockerOrchestrator instance.")
        self.image_name = None
        self.environment = None
        self.bind_mount = None
        self.read_environment_variables()
        self.docker_instance = docker.from_env()

    def read_environment_variables(self):
        """
        This function sinks in the global environment variables.
        """
        while self.image_name is None or \
                self.environment is None or \
                self.bind_mount is None:

            self.image_name = os.getenv("image_name_key",
                                        default=None)
            self.environment = os.getenv("environment_key",
                                         default=None)
            self.bind_mount = os.getenv("bind_mount_key",
                                        default=None)

        logging_to_console_and_syslog("image_name={},"
                                      "environment={},"
                                      "bind_mount={}."
                                      .format(self.image_name,
                                              self.environment,
                                              self.bind_mount))
    def yield_container(self):
        """
        This function reads all active docker containers and returns them back to the caller one container id at a time.
        """
        for container in self.docker_instance.containers.list():
            yield container.short_id

    def check_if_container_is_active(self, container_short_id):
        """
        This function checks if the passed in docker container identifier is active and running.
        It returns a boolean value True if the container is active and false if the container is not active.
        """
        is_active=False
        try:
            container = self.docker_instance.containers.get(container_short_id)
            if container and container.status == 'running':
                is_active = True
        except:
            is_active = False
        return is_active

    def stop_container(self,container_short_id):
        """
        This function stops a running docker container.
        """
        try:
            container = self.docker_instance.containers.get(container_short_id)
            if container:
                container.stop()
        except:
            return None

    def run_container(self, document):
        """
        This function starts a docker container.
        It takes the bind mount path as an argument.
        It also takes a list of environment variables as an argument.
        It returns a docker container identifier back to the calling function.
        """
        try:
            bind_mount = self.bind_mount.split()
            environment_list = self.environment.split()
            environment_list.append("rtsp_message_key={}".format(repr(document)))

            logging_to_console_and_syslog("Trying to open a container with"
                                          "image={} "
                                          "environment={} "
                                          "volumes={} "
                                          .format(self.image_name,
                                                  repr(environment_list),
                                                  repr(bind_mount)))

            container = self.docker_instance.containers.run(
                self.image_name,
                environment=environment_list,
                volumes=bind_mount,
                detach=True)

            if container:
                return container.short_id
            else:
                return None
        except:
            print("-" * 60)
            traceback.print_exc(file=sys.stdout)
            print("-" * 60)
            return None

    def cleanup(self):
        pass