#!/usr/bin/env python3
import time
import os
import sys
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

from infrastructure_components.log.log_file import logging_to_console_and_syslog


class DockerService:
    """
    This class provides a wrapper to the actual docker services API.
    """

    def __init__(self, service_name):
        self.service_name = service_name
        self.client = None

    def __get_service_instance(self):
        """
        This function helps to get service_instance for the passed in service name.
        :return:
        """
        svc_inst = None
        services = self.client.services.list()

        if not services or type(services) != list:
            logging_to_console_and_syslog("Unable to find list of services in the current environment!")
            return svc_inst

        for svc_instance in services:
            if self.service_name in svc_instance.name:
                svc_inst = svc_instance
                break

        return svc_inst

    def scale(self, replicas):
        """
        This function scales up the service to the number of replicas passed in as an argument.
        :param replicas:
        :return:
        """
        service_instance = None

        if type(replicas) != int or replicas <=0:
            logging_to_console_and_syslog("Invalid input parameter.{}"
                                          .format(replicas))
            return

        self.client = docker.from_env()

        if not self.client:
            logging_to_console_and_syslog("Unable to find docker in the current environment!")
            return

        service_instance = self.__get_service_instance()

        if not service_instance:
            logging_to_console_and_syslog("Unable to find service instance for service name {}."
                                          .format(self.service_name))
            return

        service_instance.scale(replicas)

    def get_current_number_of_containers_per_service(self):
        """
        Returns the current number of running containers for the specified service.
        :return:
        """
        current_container_count_per_service = 0

        self.client = docker.from_env()

        if not self.client:
            logging_to_console_and_syslog("Unable to find docker in the current environment!")
            return

        service_instance = self.__get_service_instance()

        if not service_instance:
            logging_to_console_and_syslog("Unable to find service instance for service name {}."
                                          .format(self.service_name))
            return

        if self.service_name in service_instance.name:
            current_container_count_per_service = len(service_instance.tasks({'desired-state': 'running'}))

        return current_container_count_per_service

