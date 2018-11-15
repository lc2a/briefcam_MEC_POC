import os
import sys
import time
import subprocess
from sys import path
from datetime import datetime
from ast import literal_eval
from collections import defaultdict
import shutil

sys.path.append("..")  # Adds higher directory to python modules path.
from log.log_file import logging_to_console_and_syslog


class RtspOperationsOnMedia:
    """
    This class provides methods to do the following:
    1. Start an RTSP video capture background process to the pre-configured IPv4 address.
    2. Stop an existing RTSP video capture process.
    3. Check if the background RTSP video capturing process is still active.
    4. Move the captured video files from the current directory to a shared directory.
    """
    def __init__(self):
        self.process_instance = None
        self.video_file_path = None
        self.rtsp_file_name_prefix = None
        self.rtsp_duration_of_the_video = 0
        self.rtsp_message = None
        self.min_file_size = 0
        self.process_id = 0
        self.rtsp_stream_arguments = None
        self.rtsp_capture_application = None
        self.couchdb_identifier = None
        self.rtsp_server_hostname = None
        self.camera_name = ""
        self.dictionary_of_values = None
        self.cwd = os.getcwd()
        self.before = []
        self.after = []
        self.video_file_name_size = defaultdict(defaultdict)
        self.read_environment_variables()
        self.max_attemtps_before_moving_a_file_to_process = 3

    def read_environment_variables(self):
        """
        Read the environment variables.
        :return:
        """
        while self.video_file_path is None and \
                self.rtsp_file_name_prefix is None and \
                self.rtsp_capture_application is None and \
                self.rtsp_duration_of_the_video is 0 and \
                self.min_file_size is 0 and \
                self.rtsp_message is None:
            time.sleep(1)
            self.video_file_path = os.getenv("video_file_path_key",
                                             default=None)
            self.rtsp_file_name_prefix = os.getenv("rtsp_file_name_prefix_key",
                                                   default=None)
            self.rtsp_capture_application = os.getenv("rtsp_capture_application_key",
                                                      default=None)
            self.rtsp_duration_of_the_video = int(os.getenv("rtsp_duration_of_the_video_key",
                                                            default=0))
            self.rtsp_message = os.getenv("rtsp_message_key", default=None)
            self.min_file_size = int(os.getenv("min_file_size_key",
                                               default=0))

        logging_to_console_and_syslog(("video_file_path={}"
                                       .format(self.video_file_path)))
        logging_to_console_and_syslog(("rtsp_file_name_prefix={}"
                                       .format(self.rtsp_file_name_prefix)))
        logging_to_console_and_syslog(("rtsp_capture_application={}"
                                       .format(self.rtsp_capture_application)))
        logging_to_console_and_syslog(("rtsp_duration_of_the_video={}"
                                       .format(self.rtsp_duration_of_the_video)))
        logging_to_console_and_syslog("cwd={}".format(self.cwd))
        logging_to_console_and_syslog("min_file_length={}".format(self.min_file_size))

    def __fetch_ip_address_from_message(self):
        """
        Use literal_eval to evaluate the rtsp_message passed in as an argument to this docker container.
        As this rtsp message is a dictionary document from couchDB, it should be be decoded as a dictionary.
        Do not process further, if the decoded object is not a dictionary.
        Extract the camera name, camera ip address and the identifiers into local variables for further processing.
        :return status:
        """
        status = False

        logging_to_console_and_syslog("Trying to decode message {}".format(self.rtsp_message))

        try:
            self.dictionary_of_values = literal_eval(self.rtsp_message)
        except:
            logging_to_console_and_syslog("Unable to evaluate message {}".format(self.rtsp_message))
            return status

        logging_to_console_and_syslog(
            "After literal_eval, "
            "type={} dictionary_of_values={}".format(type(self.dictionary_of_values),
                                                     repr(self.dictionary_of_values)))

        if type(self.dictionary_of_values) != dict:
            logging_to_console_and_syslog("Unable to decode the message")
            return status

        for name, value in self.dictionary_of_values.items():
            logging_to_console_and_syslog("name={}, value={}".format(name, value))
            if name == 'name':
                self.camera_name = value
            elif name == 'ip' or name == 'hostname':
                self.rtsp_server_hostname = value
            elif name == '_id':
                self.couchdb_identifier = value

        if not self.camera_name or not self.rtsp_server_hostname:
            logging_to_console_and_syslog("Unable to find a hostname/camera name to open RTSP stream {}:".
                                          format(self.rtsp_message))
            return status

        status = True
        return status

    def __prepare_rtsp_application_arguments(self):
        """
        1. Decode the passed in rtsp_message and convert it to a dictionary and extract the camera details(ip address,
        camera name,...)
        2. Try to open a background process to start rtsp video capture of the camera's video feed.
        :return status:
        """

        status = False
        self.rtsp_stream_arguments = None

        status = self.__fetch_ip_address_from_message()

        if not status:
            return status

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

        status = True
        return status

    def start_rtsp_stream(self):
        """
        1. This function prepares RTSP command line application argument with the previously configured arguments.
        2. This function opens a background RTSP application and returns the couchdb identifier if was successful.
        3. Return 0 if it encountered failure.
        :return status 0 - False,
                       +ve value - couchDB identifier. Success:
        """
        status = 0

        if self.rtsp_message is None:
            return status

        if self.__prepare_rtsp_application_arguments() is False:
            logging_to_console_and_syslog("Cannot parse this message {}.".format(self.rtsp_message))
            return status

        # Prepare an RTSP capture application command.
        command_list = [self.rtsp_capture_application]
        command_list += self.rtsp_stream_arguments.split()

        logging_to_console_and_syslog("Trying to open Process {} with argument {} list={}"
                                      .format(self.rtsp_capture_application,
                                              self.rtsp_stream_arguments,
                                              command_list))
        self.process_id = subprocess.Popen(command_list)
        if self.process_id is None:
            logging_to_console_and_syslog("Cannot open Process {}"
                                          .format(self.rtsp_capture_application))
            return status
        logging_to_console_and_syslog("Successfully opened {} stream to IP {}. Output={}"
                                      .format(self.rtsp_capture_application,
                                              self.rtsp_server_hostname,
                                              self.process_id))
        return self.couchdb_identifier

    def stop_rtsp_stream(self):
        """
        This function kills the background RTSP video capture application.
        :return:
        """
        if self.process_id:
            self.process_id.kill()
            self.process_id = None

    def check_rtsp_stream(self):
        """
        This function tries to grep for the background process using linux "ps aux" command.
        It returns false if it could not find the background process or it found one but the process in defunct state.
        It returns true if it found the background process and it is not in defunct state.
        :return status:
        """
        status = False
        result = subprocess.run(['ps', 'aux'], stdout=subprocess.PIPE).stdout.decode('utf-8')
        if result.find(self.rtsp_capture_application) == -1 or \
                result.find("[{}] <defunct>".format(self.rtsp_capture_application)) != -1:
            logging_to_console_and_syslog("Cannot find process {} "
                                          "running.".format(self.rtsp_capture_application))
            return status
        else:
            logging_to_console_and_syslog("process {} is "
                                          "running.".format(self.rtsp_capture_application))
            status = True
        return status

    def __getSize(self, filename):
        """
        This function invokes an OS command to fetch the current filesize of a filename.
        :param filename:
        :return filesize:
        """
        st = os.stat(filename)
        return st.st_size

    def create_new_dict_obj(self, filename, filesize):
        """
        Create a new dictionary object with size, count as the keys.
        Set the dictionary object as the value in the outer dictionary video_file_name_size
        which has filename as the key.
        :param filename:
        :param filesize:
        :return True:
        """
        # No dictionary object found for key filename.
        # Create a new dictionary and set it as the value in dict video_file_name_size.
        dict_obj = defaultdict(int)
        dict_obj["size"] = filesize
        dict_obj["count"] = 0
        self.video_file_name_size[filename] = dict_obj
        logging_to_console_and_syslog("Creating and Storing this file "
                                      "{} size {} in cache "
                                      .format(filename,
                                              filesize))
        return True

    def move_file_from_local_to_shared_directory(self, filename, filesize):
        """
        Append current date and time with the filename.
        Replace '-' with '_', ':' with '_', and '.' with '_'.
        Execute an OS command to move the file from source to destination directory.
        Delete the file if it is less than the specified minimum file length.
        :param filename:
        :param filesize:
        :return status:
        """
        status = False
        i = datetime.now()
        formatted_string = "{}_{}_{}".format(self.camera_name,
                                             i.date(),
                                             i.time()) \
            .replace('-', '_') \
            .replace(':', '_') \
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
            if filesize < self.min_file_size:
                logging_to_console_and_syslog("Deleting this file {} size {} because the file"
                                              "size is less than the min file length {}."
                                              .format(filename,
                                                      filesize,
                                                      self.min_file_size))
                os.remove(filename)
            else:
                shutil.move(filename, destination)
                del self.video_file_name_size[filename]
            status = True
        except:
            logging_to_console_and_syslog("Unable to move file{} to dst {}"
                                          .format(filename, destination))
            status = False

        return status

    def update_existing_dict_obj(self, filename, filesize):
        """
        1. Check if the filesize of the filename did not change since previous second.
        2. If the filesize did not change, give it a grace period of configured max attempts
           before moving the file from local to shared directory.
        3. If the file size has changed, store the new filesize.

        :param filename:
        :param filesize:
        :return status:
        """
        status = False

        if not filesize or not filename:
            return status

        dict_obj = self.video_file_name_size[filename]
        # check if the filesize of the filename did not change since previous second.
        if dict_obj["size"] == filesize:
            # Give it a grace period of configured max attempts
            # before moving the file from local to shared directory.
            if dict_obj["count"] == self.max_attemtps_before_moving_a_file_to_process:
                status = self.move_file_from_local_to_shared_directory(filename, filesize)
            else:
                # Bump up the counter by 1.
                dict_obj["count"] += 1
                self.video_file_name_size[filename] = dict_obj
                logging_to_console_and_syslog("current file size {} matches with "
                                              "previous value in the dict."
                                              "Incrementing the count to {} "
                                              .format(filesize,
                                                      dict_obj["count"]))
                status = True
        else:
            # The file size has changed. Store the new filesize.
            dict_obj["size"] = filesize
            dict_obj["count"] = 0
            self.video_file_name_size[filename] = dict_obj
            logging_to_console_and_syslog("Updating this file {} size {} in cache "
                                          .format(filename,
                                                  filesize))
            status = True
        return status

    def process_file_name(self, filename):
        """
        Use the filename as the key in the dictionary video_file_name_size to fetch the inner dictionary value.
        Create a new dictionary value if the key is not found.
        Update the existing dictionary value if a key/value already exists in the dictionary video_file_name_size.
        :param filename:
        :return status:
        """
        status = False

        if not filename or not type(filename) == str:
            return status

        if filename.endswith('.mp4'):
            filesize = self.__getSize(filename)
            logging_to_console_and_syslog("Found a file name that ends with .mp4 {}, "
                                          "filesize={}"
                                          .format(filename, filesize))
            if filesize == 0:
                return status

            dict_obj = self.video_file_name_size[filename]
            if not dict_obj:
                # No dictionary object found for key filename.
                # Create a new dictionary and set it as the value in dict video_file_name_size.
                status = self.create_new_dict_obj(filename, filesize)
            else:
                status = self.update_existing_dict_obj(filename, filesize)

        return status

    def move_media_files_to_shared_directory(self):
        """
        Fetch all the files in the current working directory.
        For each filename, invoke process_file_name() to process that file.
        :return status:
        """
        status = False
        return_list = os.listdir(os.getcwd())

        if return_list is None:
            return status

        logging_to_console_and_syslog("return_list={}".format(return_list))

        for filename in return_list:
            status = self.process_file_name(filename)
            if not status:
                logging_to_console_and_syslog("Processing filename {} returned {}."
                                              .format(filename,
                                                      status))
        status = True
        return status

    def cleanup(self):
        pass
