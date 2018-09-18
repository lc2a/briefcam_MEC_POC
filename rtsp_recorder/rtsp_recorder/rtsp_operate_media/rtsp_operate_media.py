import os
import sys
import time
import subprocess
from sys import path
from datetime import datetime

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog
from ast import literal_eval
from collections import defaultdict
import shutil


class RtspOperationsOnMedia:
    def __init__(self):
        self.process_instance = None
        self.video_file_path = None
        self.rtsp_file_name_prefix = None
        self.rtsp_duration_of_the_video = 0
        self.process_id = 0
        self.rtsp_stream_arguments = None
        self.rtsp_capture_application = None
        self.couchdb_identifier = None
        self.rtsp_server_hostname = None
        self.camera_name = None
        self.dictionary_of_values = None
        self.cwd = os.getcwd()
        self.before = []
        self.after = []
        self.video_file_name_size = defaultdict(defaultdict)
        self.read_environment_variables()
        self.max_attemtps_before_moving_a_file_to_process = 3

    def read_environment_variables(self):
        while self.video_file_path is None and \
                self.rtsp_file_name_prefix is None and \
                self.rtsp_capture_application is None and \
                self.rtsp_duration_of_the_video is 0:
            time.sleep(1)
            self.video_file_path = os.getenv("video_file_path_key", default=None)
            self.rtsp_file_name_prefix = os.getenv("rtsp_file_name_prefix_key", default=None)
            self.rtsp_capture_application = os.getenv("rtsp_capture_application_key", default=None)
            self.rtsp_duration_of_the_video = int(os.getenv("rtsp_duration_of_the_video_key", default=0))

        logging_to_console_and_syslog(("video_file_path={}".format(self.video_file_path)))
        logging_to_console_and_syslog(("rtsp_file_name_prefix={}".format(self.rtsp_file_name_prefix)))
        logging_to_console_and_syslog(("rtsp_capture_application={}".format(self.rtsp_capture_application)))
        logging_to_console_and_syslog(("rtsp_duration_of_the_video={}".format(self.rtsp_duration_of_the_video)))
        logging_to_console_and_syslog("cwd={}".format(self.cwd))

    def __fetch_ip_address_from_message(self, message):

        logging_to_console_and_syslog("Trying to decode message {}".format(message))

        try:
            self.dictionary_of_values = literal_eval(message)
        except:
            logging_to_console_and_syslog("Unable to evaluate message {}".format(message))
            return

        logging_to_console_and_syslog(
            "After literal_eval, "
            "type={} dictionary_of_values={}".format(type(self.dictionary_of_values),
                                                     repr(self.dictionary_of_values)))

        if type(self.dictionary_of_values) != dict:
            logging_to_console_and_syslog("Unable to decode the message")
            return

        for name, value in self.dictionary_of_values.items():
            logging_to_console_and_syslog("name={}, value={}".format(name, value))
            if name == 'name':
                self.camera_name = value
            elif name == 'ip' or name == 'hostname':
                self.rtsp_server_hostname = value
            elif name == '_id':
                self.couchdb_identifier = value

    def __prepare_rtsp_application_arguments(self, message):
        self.rtsp_stream_arguments = None
        self.__fetch_ip_address_from_message(message)
        if self.rtsp_server_hostname is None:
            logging_to_console_and_syslog("Unable to find a hostname to open RTSP stream {}:".
                                          format(message))
            return False

        """
        -D 1 # Quit if no packets for 1 second or more 
        -c # Continuously record, after completion of 
        -d timeframe 
        -B 10000000 # Input buffer of 10 MB 
        -b 10000000 # Output buffer 10MB (to file) 
        -q # Produce files in QuickTime format 
        -Q # Display QOS statistics 
        -F cam_eight # Prefix output filenames with this text 
        -d 28800 # Run openRTSP this many seconds 
        -P 900 # Start a new output file every -P seconds 
        -t # Request camera end stream over TCP, not UDP 
        -u admin 123456 # Username and password expected by camera 
        rtsp://192.168.1.108:554/11 # Camera's RTSP URL
        """
        self.rtsp_stream_arguments = "-D 3 -c -B 1000000000 -b 1000000000 -4 -F {}  -P {} rtsp://{}". \
            format(self.rtsp_file_name_prefix,
                   self.rtsp_duration_of_the_video,
                   self.rtsp_server_hostname)
        logging_to_console_and_syslog("openRTSP argument {}:".format(self.rtsp_stream_arguments))
        return True

    def start_rtsp_stream(self, message):
        if message is None:
            return 0

        if self.__prepare_rtsp_application_arguments(message) is False:
            logging_to_console_and_syslog("Cannot parse this message {}.".format(message))
            return 0


        command_list=[]
        command_list.append(self.rtsp_capture_application)
        command_list += self.rtsp_stream_arguments.split()

        logging_to_console_and_syslog("Trying to open Process {} with argument {} list={}"
                                      .format(self.rtsp_capture_application,
                                              self.rtsp_stream_arguments,
                                              command_list))
        self.process_id = subprocess.Popen(command_list)
        if self.process_id is None:
            logging_to_console_and_syslog("Cannot open Process {}".format(self.rtsp_capture_application))
            return 0
        logging_to_console_and_syslog("Successfully opened {} stream to IP {}. Output={}"
                                      .format(self.rtsp_capture_application,
                                              self.rtsp_server_hostname,
                                              self.process_id))
        return self.couchdb_identifier

    def stop_rtsp_stream(self):
        if self.process_id:
            self.process_id.kill()
            self.process_id = None

    def check_rtsp_stream(self):
        result = subprocess.run(['ps', 'aux'], stdout=subprocess.PIPE).stdout.decode('utf-8')
        if result.find(self.rtsp_capture_application) == -1 or \
            result.find("[{}] <defunct>".format(self.rtsp_capture_application)) != -1 :
            logging_to_console_and_syslog("Cannot find process {} running.".format(self.rtsp_capture_application))
            return False
        else:
            logging_to_console_and_syslog("process {} is running.".format(self.rtsp_capture_application))
            return True

    def __getSize(self, filename):
        st = os.stat(filename)
        return st.st_size

    def move_media_files_to_shared_directory(self):
        job_done = False
        return_list = os.listdir(os.getcwd())
        if return_list is None:
            return False
        logging_to_console_and_syslog("return_list={}".format(return_list))
        for filename in return_list:
            if filename.endswith('.mp4'):
                filesize = self.__getSize(filename)
                logging_to_console_and_syslog("Found a file name that ends with .mp4 {}, "
                                              "filesize={}"
                                              .format(filename, filesize))
                job_done = True
                if filesize != 0:
                    dict_obj = self.video_file_name_size[filename]
                    if dict_obj :
                        if dict_obj["size"] == filesize :
                            if dict_obj["count"] == self.max_attemtps_before_moving_a_file_to_process:
                                i = datetime.now()
                                formatted_string = "{}_{}".format(i.date(),
                                                                  i.time())\
                                                                  .replace('-', '_')\
                                                                  .replace(':','_') \
                                                                  .replace('.', '_')
                                        destination = str("{}/{}.mp4".format(self.video_file_path,
                                                                             formatted_string))
                                logging_to_console_and_syslog("Moving this file {} to {} "
                                                  "because the file size {} and {} match."
                                                  .format(filename,
                                                          destination,
                                                          self.video_file_name_size[filename],
                                                          filesize))
                                try:
                                    shutil.move(filename, destination)
                                    del self.video_file_name_size[filename]
                                except:
                                    logging_to_console_and_syslog("Unable to move file{} to dst {}"
                                                      .format(filename, destination))
                            else:
                                dict_obj["count"] += 1
                                self.video_file_name_size[filename] = dict_obj
                                logging_to_console_and_syslog("current file size {} matches with "
                                                      "previous value in the dict."
                                                      "Incrementing the count to {} "
                                                      .format(filesize,
                                                              dict_obj["count"]))
                        else:
                            dict_obj["size"] = filesize
                            dict_obj["count"] = 0
                            self.video_file_name_size[filename] = dict_obj
                            logging_to_console_and_syslog("Updating this file {} size {} in cache "
                                                          .format(filename,
                                                                  filesize))
                    else:
                        dict_obj = defaultdict(int)
                        dict_obj["size"] = filesize
                        dict_obj["count"] = 0
                        self.video_file_name_size[filename] = dict_obj
                        logging_to_console_and_syslog("Creating and Storing this file "
                                                      "{} size {} in cache "
                                                  .format(filename,
                                                          filesize))
        return job_done
