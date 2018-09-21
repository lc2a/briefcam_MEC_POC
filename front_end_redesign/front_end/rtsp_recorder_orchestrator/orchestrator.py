import time
import os
import sys
import traceback
from sys import path
import logging
import docker

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog


class RTSPRecorderOrchestrator:
    def __init__(self):
        logging_to_console_and_syslog("Initializing RTSPRecorderOrchestrator instance.")
        self.image_name = None
        self.environment = None
        self.bind_mount = None
        self.read_environment_variables()
        self.docker_instance = docker.from_env()

    def read_environment_variables(self):
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
        for container in self.docker_instance.containers.list():
            yield container.short_id

    def check_if_container_is_active(self, container_short_id):
        is_active=False
        try:
            container = self.docker_instance.containers.get(container_short_id)
            if container:
                is_active = True
        except:
            is_active = False
        return is_active

    def stop_container(self,container_short_id):
        try:
            container = self.docker_instance.containers.get(container_short_id)
            if container:
                container.stop()
        except:
            return None

    def run_container(self, document):
        try:
            bind_mount = self.bind_mount.split()
            environment_list = self.environment.split()
            environment_list.append("document={}".format(repr(document)))

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

if __name__ == "__main__":
    # debugging code.
    rtsp_orchestrator = None
    os.environ["image_name_key"] = "ssriram1978/rtsp_recorder:latest"
    os.environ["environment_key"] = "video_file_path_key=/data " \
                                    "rtsp_file_name_prefix_key=briefcam " \
                                    "rtsp_duration_of_the_video_key=30 " \
                                    "rtsp_capture_application_key=openRTSP "
    os.environ["bind_mount_key"] = "/var/run/docker.sock:/var/run/docker.sock /usr/bin/docker:/usr/bin/docker"

    try:
        rtsp_orchestrator = RTSPRecorderOrchestrator()
        #while True:
        #    time.sleep(5)
        for container_id in rtsp_orchestrator.yield_container():
            logging_to_console_and_syslog("container = {}".format(container_id))
            logging_to_console_and_syslog("check_if_container_is_active(\"{}\") returned {}."
                                          .format(container_id,
                                                  rtsp_orchestrator.check_if_container_is_active(container_id)))

        logging_to_console_and_syslog("check_if_container_is_active(\"xyz\") returned {}."
                                      .format(rtsp_orchestrator.check_if_container_is_active("xyz")))

        container_id = rtsp_orchestrator.run_container("{foo:bar}")
        if container_id:
            logging_to_console_and_syslog("Successfully created a container with id = {}".format(container_id))
            time.sleep(60)
            rtsp_orchestrator.stop_container(container_id)
        else:
            logging_to_console_and_syslog("Failed to create a container.")

    except KeyboardInterrupt:
        logging_to_console_and_syslog("You terminated the program by pressing ctrl + c")
    except BaseException:
        logging_to_console_and_syslog("Base Exception occurred {}.".format(sys.exc_info()[0]))
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
        time.sleep(5)
    except:
        logging_to_console_and_syslog("Unhandled exception {}.".format(sys.exc_info()[0]))
        print("Exception in user code:")
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
        time.sleep(5)
    finally:
        if rtsp_orchestrator:
            rtsp_orchestrator.cleanup()