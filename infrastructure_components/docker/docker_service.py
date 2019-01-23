#!/usr/bin/env python3
import time
import os
import sys
import docker
import subprocess
import re


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

    def __get_service_instance(self):
        """
        This function helps to get service_instance for the passed in service name.
        :return:
        """
        svc_inst = None

        client = docker.from_env()

        services = client.services.list()

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

        client = docker.from_env()

        if not client:
            logging_to_console_and_syslog("Unable to find docker in the current environment!")
            return

        service_instance = self.__get_service_instance()

        if not service_instance:
            logging_to_console_and_syslog("Unable to find service instance for service name {}."
                                          .format(self.service_name))
            return

        logging_to_console_and_syslog("Scaling {} by {}.".format(self.service_name, replicas))

        service_instance.scale(replicas)

    def get_current_number_of_containers_per_service(self):
        """
        Returns the current number of running containers for the specified service.
        :return:
        """
        current_container_count_per_service = 0

        client = docker.from_env()

        if not client:
            logging_to_console_and_syslog("Unable to find docker in the current environment!")
            return current_container_count_per_service

        service_instance = self.__get_service_instance()

        if not service_instance:
            logging_to_console_and_syslog("Unable to find service instance for service name {}."
                                          .format(self.service_name))
            return current_container_count_per_service

        if self.service_name in service_instance.name:
            current_container_count_per_service = \
                len(service_instance.tasks({'desired-state': ['running', 'ready', 'accepted']}))

        logging_to_console_and_syslog("Found current_container_count_per_service = {}"
                                      .format(current_container_count_per_service))

        return current_container_count_per_service

    def get_service_id_from_service_name(self):
        """
        Example:
        docker service ls
ID                  NAME                                MODE                REPLICAS            IMAGE                                              PORTS
txspuuxz7b59        briefcam_broker                     replicated          1/1                 confluentinc/cp-enterprise-kafka:latest            *:9092->9092/tcp, *:29092->29092/tcp
4n9wxvsf4p9c        briefcam_connect                    replicated          1/1                 confluentinc/kafka-connect-datagen:0.1.0           *:8083->8083/tcp
tb2qo53pwqxb        briefcam_control-center             replicated          1/1                 confluentinc/cp-enterprise-control-center:latest   *:9021->9021/tcp
rkhnftbgya6o        briefcam_couchdb                    replicated          1/1                 couchdb:latest                                     *:5984->5984/tcp
jeh9e17qonag        briefcam_elk                        replicated          1/1                 ssriram1978/elk:latest                             *:5044->5044/tcp, *:5601->5601/tcp, *:9200->9200/tcp
kcx7ulh4g4ln        briefcam_fauxton                    replicated          1/1                 3apaxicom/fauxton:latest                           *:8000->8000/tcp
tf2yajnl7m2i        briefcam_filebeat                   replicated          1/1                 docker.elastic.co/beats/filebeat:6.4.1
12le20r3cxng        briefcam_front_end                  replicated          1/1                 ssriram1978/front_end:latest
wldmwpem0kxp        briefcam_jenkins                    replicated          1/1                 ssriram1978/jenkins:latest                         *:8082->8080/tcp, *:50001->50001/tcp
bqjni8c6z4p0        briefcam_job_dispatcher             replicated          1/1                 ssriram1978/job_dispatcher:latest
r5fljwx23rlx        briefcam_ksql-cli                   replicated          1/1                 confluentinc/cp-ksql-cli:latest
pyjnjzowd0h0        briefcam_ksql-datagen               replicated          0/1                 confluentinc/ksql-examples:latest
kb076ux9cf5q        briefcam_ksql-server                replicated          1/1                 confluentinc/cp-ksql-server:latest                 *:8088->8088/tcp
6ochthfmh1nh        briefcam_machine_learning_workers   replicated          20/20               ssriram1978/machine_learning_workers2:latest       *:5903-5923->5900/tcp
s2qszltsr104        briefcam_portainer                  replicated          1/1                 portainer/portainer:latest                         *:9000->9000/tcp
y0ko1mgb62rh        briefcam_redis                      replicated          1/1                 redis:latest                                       *:6379->6379/tcp
i1csbx7x4ekn        briefcam_redis-commander            replicated          1/1                 rediscommander/redis-commander:latest              *:9010->8081/tcp
w9z1nh3vtb7v        briefcam_rest-proxy                 replicated          1/1                 confluentinc/cp-kafka-rest:latest                  *:8084->8084/tcp
k668f80t7sa7        briefcam_schema-registry            replicated          1/1                 confluentinc/cp-schema-registry:latest             *:8081->8081/tcp
k22h646bxl8l        briefcam_zookeeper                  replicated          1/1                 confluentinc/cp-zookeeper:latest                   *:2181->2181/tcp

        :param service_name:
        :return:
        """
        service_id = None
        completedProcess = subprocess.run(["docker",
                                           "service",
                                           "ls"],
                                          stdout=subprocess.PIPE)
        if not completedProcess:
            return service_id

        string_formatted_output = completedProcess.stdout.decode('utf8')
        list_of_docker_service_info = string_formatted_output.split()
        for index, svc_name in enumerate(list_of_docker_service_info):
            if self.service_name in svc_name:
                service_id = list_of_docker_service_info[index-1]
        return service_id

    def count_list_of_containers_per_service(self, service_id):
        """
        Example:
        docker service ps 6ochthfmh1nh
ID                  NAME                                      IMAGE                                          NODE                          DESIRED STATE       CURRENT STATE          ERROR               PORTS
q7xbgho7ax18        briefcam_machine_learning_workers.1       ssriram1978/machine_learning_workers2:latest   mecpoc-ProLiant-BL460c-Gen9   Running             Running 3 hours ago
edrp5lpmbmhd         \_ briefcam_machine_learning_workers.1   ssriram1978/machine_learning_workers2:latest   mecpoc-ProLiant-BL460c-Gen9   Shutdown            Shutdown 3 hours ago
yrj4ykq2erj5        briefcam_machine_learning_workers.2       ssriram1978/machine_learning_workers2:latest   mecpoc-ProLiant-BL460c-Gen9   Running             Running 3 hours ago
czswggs7czq6        briefcam_machine_learning_workers.3       ssriram1978/machine_learning_workers2:latest   mecpoc-ProLiant-BL460c-Gen9   Running             Running 3 hours ago
mh3tgn0qe319        briefcam_machine_learning_workers.4       ssriram1978/machine_learning_workers2:latest   mecpoc-ProLiant-BL460c-Gen9   Running             Running 3 hours ago
pn6ci51z1g2g        briefcam_machine_learning_workers.5       ssriram1978/machine_learning_workers2:latest   mecpoc-ProLiant-BL460c-Gen9   Running             Running 3 hours ago
        :param service_id:
        :return:
        """
        occurance_count = 0
        completedProcess = subprocess.run(["docker",
                                           "service",
                                           "ps",
                                           service_id],
                                          stdout=subprocess.PIPE)
        if not completedProcess:
            return occurance_count
        output = completedProcess.stdout.decode('utf8')
        occurance = re.findall(self.service_name, output)
        #print("occurance={},len={}".format(occurance,len(occurance)))
        occurance_count = len(occurance)
        return occurance_count
