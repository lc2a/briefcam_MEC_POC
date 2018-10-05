import pyautogui
import os
import subprocess
import time
import sys
from subprocess import check_output
import signal

sys.path.append("..") # Adds higher directory to python modules path.

from log.log_file import logging_to_console_and_syslog
from redisClient.RedisClient import RedisClient

search_coordinates = (840, 200)
case_coordinates = (83, 565)
add_video_coordinates = (945, 159)
same_camera_coordinates = (673, 580)
same_camera_next_coordinates = (971, 866)
browse_for_file_location_coordinates = (541, 618)
next_after_uploading_video_coordinates = (971,841)
process_uploading_video_coordinates = (971,862)
cancel_coordinates = (980, 287)
confirm_cancel_coordinates = (717,607)
back_coordinates = (36, 69)
create_case_coordinates = (929, 202)
cancel_search_coordinates = (842,163)

class BriefCamClickTimeoutException(Exception):
    def __init__(self):
        event = "BriefCam Server click timeout occurred."
        logging_to_console_and_syslog(event)
        self.redis_instance.write_an_event_on_redis_db(event)
        self.redis_instance.write_an_event_on_redis_db(event, self.redis_log_keyname)
        pyautogui.press('esc')
        pyautogui.press('esc')
        raise BriefCamServerException

class BriefCamServerException(Exception):
    def __init__(self):
        event = "BriefCam Server exception occurred."
        logging_to_console_and_syslog(event)
        self.redis_instance.write_an_event_on_redis_db(event)
        self.redis_instance.write_an_event_on_redis_db(event, self.redis_log_keyname)

class BriefCamNoProcessExcept(Exception):
    def __init__(self):
        event = "BriefCam Server No process occurred."
        logging_to_console_and_syslog(event)
        self.redis_instance.write_an_event_on_redis_db(event)
        self.redis_instance.write_an_event_on_redis_db(event, self.redis_log_keyname)
        raise BriefCamServerException

class UploadVideoToBriefCam():
    pyautogui.PAUSE = 0.1
    def filename_formatted(self, filename):
        if __name__ == "__main__":
            return filename
        else:
            return os.path.dirname(os.path.realpath(__file__)) + "/" + self.image_directory + "/" + filename

    def __init__(self):
        self.case_name = None
        self.case_url = None
        self.browser_loc = None
        self.username = None
        self.password = None
        self.image_directory = None
        self.process = None
        self.browser_ready = False
        self.browser_name = None
        self.max_retry_attempts = 0
        self.sleep_time = 1
        self.time_for_browser_to_open = 60
        self.time_between_input_character = 0
        self.redis_log_keyname = None
        self.total_job_done_count_redis_name = None
        self.redis_instance = RedisClient()
        self.import_environment_variables()
        self.prepare_browser()

    def write_log_to_redis_and_logging_framework(self, event,write_to_redis_event_summary=False):
        logging_to_console_and_syslog(event)
        self.redis_instance.write_an_event_on_redis_db(event)
        if write_to_redis_event_summary:
            self.redis_instance.write_an_event_on_redis_db(event, self.redis_log_keyname)

    def import_environment_variables(self):
        while self.case_name is None or \
            self.case_url is None or \
            self.browser_name is None or \
            self.browser_loc is None or \
            self.username is None or \
            self.password is None or \
            self.image_directory is None or \
            self.max_retry_attempts is 0 or \
            self.sleep_time is 0 or \
            self.time_between_input_character is 0 or \
            self.redis_log_keyname is None or \
            self.time_for_browser_to_open is 0 :

            time.sleep(self.sleep_time)
            self.case_name = os.getenv("case_name_key", default=None)
            self.case_url = os.getenv("case_url_key", default=None)
            self.browser_name = os.getenv("browser_name_key", default=None)
            self.browser_loc = os.getenv("browser_loc_key", default=None)
            self.username = os.getenv("login_username_key", default=None)
            self.password = os.getenv("login_password_key", default=None)
            self.image_directory = os.getenv("image_directory", default=None)
            self.max_retry_attempts = int(os.getenv("max_retry_attempts_key", default=0))
            self.sleep_time = int(os.getenv("sleep_time_key", default=0))
            self.time_between_input_character = float(os.getenv("time_between_input_character_key", default=0))
            self.time_for_browser_to_open = int(os.getenv("time_for_browser_to_open_key", default=0))
            self.total_job_done_count_redis_name = os.getenv("total_job_done_count_redis_name_key", default=None)
            self.redis_log_keyname = os.getenv("redis_log_keyname_key",
                                               default=None)

        self.write_log_to_redis_and_logging_framework("password={}"
                                                      .format(self.password),True)
        self.write_log_to_redis_and_logging_framework("case_name={}"
                                                      .format(self.case_name),True)
        self.write_log_to_redis_and_logging_framework("case_url={}"
                                                      .format(self.case_url),True)
        self.write_log_to_redis_and_logging_framework("username={}"
                                                      .format(self.username))
        self.write_log_to_redis_and_logging_framework("browser_loc={}"
                                                      .format(self.browser_loc),True)
        self.write_log_to_redis_and_logging_framework("browser_name={}"
                                                      .format(self.browser_name),True)
        self.write_log_to_redis_and_logging_framework("image_directory={}"
                                                      .format(self.image_directory),True)
        self.write_log_to_redis_and_logging_framework("max_retry_attempts={}"
                                                      .format(self.max_retry_attempts),True)
        self.write_log_to_redis_and_logging_framework("sleep_time={}"
                                                      .format(self.sleep_time),True)
        self.write_log_to_redis_and_logging_framework("time_between_input_character={}"
                                                      .format(self.time_between_input_character),True)
        self.write_log_to_redis_and_logging_framework("time_for_browser_to_open={}"
                                                      .format(self.time_for_browser_to_open),True)
        self.write_log_to_redis_and_logging_framework("total_job_done_count_redis_name={}"
                                                      .format(self.total_job_done_count_redis_name),True)
        self.write_log_to_redis_and_logging_framework("redis_log_keyname={}"
                                      .format(self.redis_log_keyname),True)

    def __proceed_with_execution(self):
        # pyautogui.alert('Shall I proceed in creating a case?')
        text = pyautogui.confirm(text='Shall I proceed in creating a case?', title='Question', buttons=['OK', 'Cancel'])
        if text == 'Cancel':
            return False
        return True

    def _left_click_this_coordinate(self, coordinates):
        if coordinates is None:
            return
        event_log = "Trying to left click this coordinate[{},{}] ".format(coordinates[0], coordinates[1])
        self.write_log_to_redis_and_logging_framework(event_log)
        pyautogui.click(x=coordinates[0], y=coordinates[1])

    def __left_click_this_image(self, button_name, force_wait=True):
        button_location = None
        current_retry_count = 0
        while button_location is None:
            event = "Trying to match " + button_name
            self.write_log_to_redis_and_logging_framework(event)
            try:
                button_location = pyautogui.locateOnScreen(button_name, grayscale=True)
            except FileNotFoundError:
                event = "File {} Not Found".format(button_name)
                self.write_log_to_redis_and_logging_framework(event)
                raise FileNotFoundError
            if button_location is None:
                if not force_wait:
                    return False
                elif current_retry_count >= self.max_retry_attempts:
                    event_log = "For button name {} Retry attempts {} is " \
                            "greater than max retries {}".format(button_name,
                                                          current_retry_count,
                                                          self.max_retry_attempts)
                    self.write_log_to_redis_and_logging_framework(event_log)
                    raise BriefCamClickTimeoutException
                else:
                    current_retry_count += 1
                    time.sleep(self.sleep_time)

        buttonx, buttony = pyautogui.center(button_location)
        event_log = "Clicking buttonx={} and buttony={}".format(buttonx, buttony)
        self.write_log_to_redis_and_logging_framework(event_log)
        pyautogui.click(buttonx, buttony)
        return True

    def __login_to_briefcam_server(self):
        pyautogui.press('esc')
        for index in range(5):
            return_value = self.__left_click_this_image(self.filename_formatted('signin_button.png'), False)
            if return_value:
                break
        if return_value:
            pyautogui.hotkey('tab')
            time.sleep(self.time_between_input_character)
            pyautogui.typewrite(self.username, interval=self.time_between_input_character)
            pyautogui.hotkey('tab')
            time.sleep(self.time_between_input_character)
            pyautogui.typewrite(self.password, interval=self.time_between_input_character)
            pyautogui.press('enter')  # press the Enter key
            pyautogui.press('esc')
            pyautogui.press('esc')

    def __extract_case_name_from_video_file_name(self,filename):
        if filename is None:
            event = "Filename is None"
            self.write_log_to_redis_and_logging_framework(event)
            return

        starting_index = filename.rfind('/')
        if starting_index == -1:
            event = "Unable to find / in the filename {}.".format(filename)
            self.write_log_to_redis_and_logging_framework(event)
            return

        ending_index = filename.rfind(".mp4")
        if ending_index == -1:
            self.write_log_to_redis_and_logging_framework(
                "Unable to find .mp4 in the filename {}.".format(filename))
            return

        case_name = filename[starting_index+1:ending_index]
        case_name_list = case_name.split('_')
        if len(case_name_list) <=0:
            self.write_log_to_redis_and_logging_framework(
                "Unable to find _ in the filename {}.".format(filename))
            return
        # Example: case_name = camera1_2018_10_03_10_03_15_282182.mp4
        #Discard the microseconds, seconds and minutes from the casename.
        # Just include camera name, date, hour in the case_name.
        # Just create casename every hour and group videos based on every hour.
        self.case_name = '_'.join(case_name_list[0:len(case_name_list) - 3])

        self.write_log_to_redis_and_logging_framework(
            "Prepared casename {}.".format(self.case_name))

    def __check_for_background_process(self):
        result = subprocess.run(['ps', 'aux'], stdout=subprocess.PIPE).stdout.decode('utf-8')
        if self.browser_name in result:
            self.write_log_to_redis_and_logging_framework("process {} is "
                                          "running.".format(self.browser_name))
            return True
        else:
            self.write_log_to_redis_and_logging_framework("Cannot find process {} "
                                          "running.".format(self.browser_name))
            return False

    def __create_case_for_the_first_time(self):
        # MEC-POC case is getting created for the first time.
        self._left_click_this_coordinate(create_case_coordinates)
        time.sleep(self.time_between_input_character)
        pyautogui.typewrite(self.case_name,
                            interval=0.1)  # prints out the case name with a quarter second delay after each character
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.press('enter')  # press the Enter key
        time.sleep(self.time_between_input_character)
        self._left_click_this_coordinate(case_coordinates)

    def __search_and_leftclick_case(self, filename):
        no_results_found = None
        pyautogui.press('esc')
        pyautogui.press('esc')
        self.__extract_case_name_from_video_file_name(filename)
        pyautogui.hotkey('esc')
        time.sleep(self.sleep_time)
        self._left_click_this_coordinate(search_coordinates)
        time.sleep(self.time_between_input_character)
        pyautogui.typewrite(self.case_name, interval=0.1)
        self.write_log_to_redis_and_logging_framework(
            "Clicking on case at location:{}".format(case_coordinates))
        time.sleep(self.sleep_time)
        self._left_click_this_coordinate(case_coordinates)

    def __leftclick_add_video_button(self):
        return_value = False
        for index in range(4):
            return_value = self.__left_click_this_image(
                self.filename_formatted('add_video_to_case_button.png'), False)
            pyautogui.press('esc')
            if return_value:
                break
            time.sleep(self.time_between_input_character)
        return return_value

    def __write_case_name_in_redis(self,case_name):
        self.redis_instance.write_an_event_on_redis_db(case_name)

    def __make_sure_you_could_click_add_video_button(self):
        add_video_button_found = False
        current_retry_count = 0
        while not add_video_button_found:
            if not self.browser_alive():
                break
            add_video_button_found = self.__leftclick_add_video_button()
            if not add_video_button_found:
                self.write_log_to_redis_and_logging_framework(
                    "Add video button is not found for case name {}."
                                          .format(self.case_name))
                #look up in redisClient db to check if the case name exists.
                # if the case name already exists in redisDB, then, keep trying until you find the casename.
                # else create the case name and update the same in redisClient DB.
                if self.redis_instance.check_if_the_key_exists_in_redis_db(self.case_name):
                    self.write_log_to_redis_and_logging_framework(
                        "Case name {} already exists. Retrying after a second.."
                                                  .format(self.case_name))
                    self._left_click_this_coordinate(cancel_search_coordinates)
                    current_retry_count+=1
                    if current_retry_count >=self.max_retry_attempts:
                        break
                    else:
                        time.sleep(self.sleep_time)
                else:
                    self.redis_instance.set_the_key_in_redis_db(self.case_name)
                    self._left_click_this_coordinate(cancel_search_coordinates)
                    pyautogui.press('esc')
                    self.__create_case_for_the_first_time()
        return add_video_button_found

    def __add_video(self, file_name):
        add_video_successful = False
        if not self.__make_sure_you_could_click_add_video_button():
            return add_video_successful
        self._left_click_this_coordinate(same_camera_coordinates)
        time.sleep(self.sleep_time)
        self._left_click_this_coordinate(same_camera_next_coordinates)
        time.sleep(self.sleep_time)
        self._left_click_this_coordinate(browse_for_file_location_coordinates)
        time.sleep(self.sleep_time)
        pyautogui.typewrite(file_name, interval=0.1)
        pyautogui.press('enter')  # press the Enter key
        # left_click_this_image('open_button.png')
        self.__left_click_this_image(self.filename_formatted('next2_button.png'))
        time.sleep(self.sleep_time)
        self._left_click_this_coordinate(process_uploading_video_coordinates)
        time.sleep(self.sleep_time)
        # pyautogui.hotkey('tab')
        #pyautogui.press('enter')  # press the Enter key
        add_video_successful = True
        return add_video_successful

    def __make_sure_video_is_added_successfully(self):
        self._left_click_this_coordinate(cancel_coordinates)
        time.sleep(self.sleep_time)
        pyautogui.press('esc')
        self._left_click_this_coordinate(confirm_cancel_coordinates)
        time.sleep(self.sleep_time)
        pyautogui.press('esc')
        time.sleep(self.sleep_time)
        pyautogui.press('esc')

    def __find_and_close_unwanted_popup(self):
        # self.__left_click_this_image(self.filename_formatted("restore_pages_button.png"), False)
        pyautogui.hotkey('esc')
        pyautogui.hotkey('esc')

    def __open_browser(self):
        process_id = subprocess.Popen([self.browser_loc, self.case_url])
        time.sleep(self.time_for_browser_to_open)
        self.__find_and_close_unwanted_popup()
        return process_id

    def __close_browser(self):
        if self.process is None:
            return
        self.process.kill()
        self.process=None
        for pid in map(int, check_output(["pidof", self.browser_name]).split()):
            os.kill(pid, signal.SIGTERM)

    def browser_alive(self):
        is_browser_alive = False
        if self.__check_for_background_process():
            is_browser_alive = True
        return is_browser_alive

    def go_to_main_screen(self):
        self._left_click_this_coordinate(back_coordinates)
        time.sleep(self.sleep_time)

    def prepare_browser(self,skip_login=False):
        if self.process is None:
            self.process = self.__open_browser()
            if self.process is None:
                self.write_log_to_redis_and_logging_framework("Process is None")
                raise BriefCamNoProcessExcept(self.process)
            if not skip_login:
                self.__login_to_briefcam_server()
            self.browser_ready = True

    def clean_up(self):
        self.browser_ready = False
        # close the browser and reopen it.
        self.write_log_to_redis_and_logging_framework(
            "Closing and reopening web browser.",True)
        self.__close_browser()
        self.case_name = ""
        self.prepare_browser(skip_login=True)

    def __delete_video_clip_from_shared_volume(self, file_name):
        self.write_log_to_redis_and_logging_framework(
            "Deleting file_name {}".format(file_name))
        os.remove(file_name)

    def process_new_file(self, file_name):
        job_done = False
        while job_done == False:
            try:
                while not self.browser_ready:
                    self.write_log_to_redis_and_logging_framework(
                        "Waiting for the browser to be ready")
                    time.sleep(self.sleep_time)

                self.__search_and_leftclick_case(file_name)

                if not self.__add_video(file_name):
                    self.write_log_to_redis_and_logging_framework(
                        "Add video failed. Closing and reopening the browser.",True)
                    self.clean_up()
                    continue
                self.__make_sure_video_is_added_successfully()
                self.__delete_video_clip_from_shared_volume(file_name)
                self.go_to_main_screen()

                if not self.browser_alive():
                    self.write_log_to_redis_and_logging_framework(
                        "Browser is not alive. Reopening the browser.",True)
                    self.clean_up()
                job_done = True
            except BriefCamServerException:
                self.clean_up()
# self.__close_browser(process)


if __name__ == "__main__":
    # define your unit test cases here.
    # obj=UploadVideoToBriefCam()
    # obj.process_new_file("test")
    # Brieobj.filename_formatted("test")
    pass
