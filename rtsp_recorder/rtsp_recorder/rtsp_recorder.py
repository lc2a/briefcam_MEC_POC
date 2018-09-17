#!/usr/bin/env python3
import time
import os
import sys
import traceback
from sys import path

path.append(os.getcwd())
from log.log_file import logging_to_console_and_syslog
from kafka_consumer.kafkaconsumer import Consumer
from couchdb_client.couchdb_client import CouchDBClient
from rtsp_operate_media.rtsp_operate_media import RtspOperationsOnMedia


class RtspRecorder:
    def __init__(self):
        self.kafka_consumer_instance = None
        self.couchdb_client_instance = None
        self.rtsp_media_instance = None
        self.initialize_instances()

    def initialize_instances(self):
        self.rtsp_media_instance = RtspOperationsOnMedia()
        self.couchdb_client_instance = CouchDBClient()
        self.kafka_consumer_instance = Consumer()

    def perform_operation(self):
        # 1. Poll for a new kafka message.
        message = None
        message = self.kafka_consumer_instance.poll_for_new_message
        if message is None:
            return

        logging_to_console_and_syslog("Received message {} from kakfaQ.".format(message))

        # 2. Instruct RTSP to start capturing video.
        message_id = self.rtsp_media_instance.start_rtsp_stream(message)

        if message_id == 0:
            logging_to_console_and_syslog("Unable to start RTSP stream {} from kakfaQ.".format(message))
            raise BaseException

        # 3. Update the containerID in couchDB.
        self.couchdb_client_instance.update_container_id(message_id)

        continue_capturing_video = True
        while continue_capturing_video:
            # 4. Every second,
            # a. If the camera entry still exist in couchDB, then,
            # 1. Make sure that the video capture is still ongoing.
            # 2. Make sure to move the media files periodically into shared media mount.
            # b. If the camera entry does not exist in couchDB, then.
            # 1. Stop capturing video.
            time.sleep(1)
            if self.couchdb_client_instance.is_the_document_still_valid():
                logging_to_console_and_syslog("The document {} is still valid.".format(message))
                while not self.rtsp_media_instance.check_rtsp_stream():
                    logging_to_console_and_syslog("Trying to reopen the RTSP stream..".format(message))
                    self.rtsp_media_instance.stop_rtsp_stream()
                    time.sleep(1)
                    self.rtsp_media_instance.start_rtsp_stream(message)
                self.rtsp_media_instance.move_media_files_to_shared_directory()
            else:
                logging_to_console_and_syslog("The document {} is invalid.".format(message))
                self.rtsp_media_instance.stop_rtsp_stream()

    def cleanup(self):
        self.kafka_consumer_instance.close_kafka_instance()


if __name__ == "__main__":
    while True:
        rtsp_recorder_instance = None
        try:
            rtsp_recorder_instance = RtspRecorder()
            while True:
                time.sleep(1)
                rtsp_recorder_instance.perform_operation()
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
            if rtsp_recorder_instance:
                rtsp_recorder_instance.cleanup()

