import pyautogui
import os
import subprocess
import time
import sys
import datetime

sys.path.append("..") # Adds higher directory to python modules path.

from log.log_file import logging_to_console_and_syslog

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

class BriefCamClickTimeoutException(Exception):
    def __init__(self):
        logging_to_console_and_syslog("BriefCam Server click timeout occurred.")
        pyautogui.press('esc')
        pyautogui.press('esc')
        raise BriefCamServerException

class BriefCamServerException(Exception):
    def __init__(self):
        logging_to_console_and_syslog("BriefCam Server exception occurred.")

class BriefCamNoProcessExcept(Exception):
    def __init__(self):
        logging_to_console_and_syslog("BriefCam Server No process occurred.")
        raise BriefCamServerException

class UploadVideoToBriefCam():
    pyautogui.PAUSE = 0.1

    def filename_formatted(self, filename):
        if __name__ == "__main__":
            # print(os.path.dirname(os.path.realpath(__file__)) + "/" + self.image_directory + "/" + filename)
            return filename
        else:
            return os.path.dirname(os.path.realpath(__file__)) + "/" + self.image_directory + "/" + filename

    def __init__(self):
        self.video_filename = None
        self.case_name = None
        self.case_url = None
        self.browser_loc = None
        self.username = None
        self.password = None
        self.image_directory = None
        self.process = None
        self.browser_ready = False
        self.browser_name = None
        self.import_environment_variables()
        self.prepare_browser()

    def import_environment_variables(self):
        while self.case_name == None:
            time.sleep(2)
            logging_to_console_and_syslog("Trying to read the environment variables")
            self.case_name = os.getenv("case_name_key", default=None)
            self.case_url = os.getenv("case_url_key", default=None)
            self.browser_name = os.getenv("browser_name_key", default=None)
            self.browser_loc = os.getenv("browser_loc_key", default=None)
            self.username = os.getenv("login_username_key", default=None)
            self.password = os.getenv("login_password_key", default=None)
            self.image_directory = os.getenv("image_directory", default=None)
        logging_to_console_and_syslog("password={}".format(self.password))
        logging_to_console_and_syslog("case_name={}".format(self.case_name))
        logging_to_console_and_syslog("case_url={}".format(self.case_url))
        logging_to_console_and_syslog("username={}".format(self.username))
        logging_to_console_and_syslog("browser_loc={}".format(self.browser_loc))
        logging_to_console_and_syslog("browser_name={}".format(self.browser_name))
        logging_to_console_and_syslog("image_directory={}".format(self.image_directory))

    def __proceed_with_execution(self):
        # pyautogui.alert('Shall I proceed in creating a case?')
        text = pyautogui.confirm(text='Shall I proceed in creating a case?', title='Question', buttons=['OK', 'Cancel'])
        if text == 'Cancel':
            return False
        return True

    max_retries = 4

    def _left_click_this_coordinate(self, coordinates):
        if coordinates is None:
            return
        logging_to_console_and_syslog("Trying to left click this coordinate[{},{}] ".format(coordinates[0],
                                                                                            coordinates[1]))
        pyautogui.click(x=coordinates[0], y=coordinates[1])

    def __left_click_this_image(self, button_name, force_wait=True):
        button_location = None
        current_retry_count = 0
        while button_location == None:
            logging_to_console_and_syslog("Trying to match " + button_name)
            try:
                button_location = pyautogui.locateOnScreen(button_name, grayscale=True)
            except FileNotFoundError:
                logging_to_console_and_syslog("File {} Not Found".format(button_name))
                raise FileNotFoundError
                break
            if button_location == None:
                if force_wait == False:
                    return False
                elif current_retry_count >= UploadVideoToBriefCam.max_retries:
                    raise BriefCamClickTimeoutException
                else:
                    current_retry_count += 1
                    #time.sleep(5)
        logging_to_console_and_syslog("button_name={},location={}".format(button_name, button_location))
        buttonx, buttony = pyautogui.center(button_location)
        logging_to_console_and_syslog("buttonx={} and buttony={}".format(buttonx, buttony))
        pyautogui.click(buttonx, buttony)
        return True

    def __login_to_briefcam_server(self):
        pyautogui.press('esc')
        for index in range(5):
            return_value = self.__left_click_this_image(self.filename_formatted('signin_button.png'), False)
            if return_value == True:
                break
        if return_value == True:
            pyautogui.hotkey('tab')
            time.sleep(0.1)
            pyautogui.typewrite(self.username, interval=0.1)
            pyautogui.hotkey('tab')
            time.sleep(0.1)
            pyautogui.typewrite(self.password, interval=0.1)
            pyautogui.press('enter')  # press the Enter key
            pyautogui.press('esc')
            pyautogui.press('esc')

    def __extract_case_name_from_video_file_name(self,filename):
        if filename is None:
            logging_to_console_and_syslog("Filename is None")
            return

        starting_index = filename.rfind('/')
        if starting_index == -1:
            logging_to_console_and_syslog("Unable to find / in the filename {}.".format(filename))
            return

        ending_index = filename.find('_',starting_index)
        if ending_index == -1:
            logging_to_console_and_syslog("Unable to find _ in the filename {}.".format(filename))
            return

        self.case_name = filename[starting_index+1:ending_index]
        now = datetime.datetime.now()
        self.case_name = "{}_{}_{}_{}_{}".format(self.case_name,
                                       now.year,
                                       now.month,
                                       now.day,
                                       now.hour)

        logging_to_console_and_syslog("Prepared casename {}.".format(self.case_name))

    def __check_for_background_process(self,process_name):
        result = subprocess.run(['ps', 'aux'], stdout=subprocess.PIPE).stdout.decode('utf-8')
        if result.find(process_name) == -1 or \
                result.find("[{}] <defunct>".format(self.browser_name)) != -1:
            logging_to_console_and_syslog("Cannot find process {} "
                                          "running.".format(self.browser_name))
            return False
        else:
            logging_to_console_and_syslog("process {} is "
                                          "running.".format(self.browser_name))
            return True

    def __create_case_for_the_first_time(self):
        # MEC-POC case is getting created for the first time.
        #self.__left_click_this_image(self.filename_formatted('create_case_button.png'))
        self._left_click_this_coordinate(create_case_coordinates)
        time.sleep(0.1)
        pyautogui.typewrite(self.case_name,
                            interval=0.1)  # prints out the case name with a quarter second delay after each character
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.press('enter')  # press the Enter key
        time.sleep(0.1)
        self._left_click_this_coordinate(case_coordinates)

    def __search_and_leftclick_case(self, filename):
        no_results_found = None
        pyautogui.press('esc')
        pyautogui.press('esc')
        self.__extract_case_name_from_video_file_name(filename)
        self._left_click_this_coordinate(search_coordinates)
        time.sleep(0.1)
        pyautogui.typewrite(self.case_name, interval=0.1)
        logging_to_console_and_syslog("Clicking on case at location:{}".format(case_coordinates))
        self._left_click_this_coordinate(case_coordinates)

    def __leftclick_add_video_button(self):
        return_value = False
        for index in range(4):
            return_value = self.__left_click_this_image(self.filename_formatted('add_video_to_case_button.png'), False)
            pyautogui.press('esc')
            if return_value:
                break
            time.sleep(0.1)
        return return_value


    def __make_sure_you_could_click_add_video_button(self):
        add_video_button_found = False
        while not add_video_button_found:
            add_video_button_found = self.__leftclick_add_video_button()
            if not add_video_button_found:
                logging_to_console_and_syslog("Add video button is not found. Creating the casename. {}."
                                          .format(self.case_name))
                self.__create_case_for_the_first_time()

    def __add_video(self, file_name):
        self.__make_sure_you_could_click_add_video_button()
        #self.__left_click_this_image(self.filename_formatted('same_camera_button.png'), False)
        #self.__left_click_this_image(self.filename_formatted('next_button.png'))
        #self.__left_click_this_image(self.filename_formatted('browse_button.png'))
        self._left_click_this_coordinate(same_camera_coordinates)
        time.sleep(1)
        self._left_click_this_coordinate(same_camera_next_coordinates)
        time.sleep(1)
        self._left_click_this_coordinate(browse_for_file_location_coordinates)
        time.sleep(1)
        pyautogui.typewrite(file_name, interval=0.1)
        pyautogui.press('enter')  # press the Enter key
        # left_click_this_image('open_button.png')
        self.__left_click_this_image(self.filename_formatted('next2_button.png'))
        #self._left_click_this_coordinate(next_after_uploading_video_coordinates)
        time.sleep(1)
        self._left_click_this_coordinate(process_uploading_video_coordinates)
        time.sleep(0.1)
        # pyautogui.hotkey('tab')
        #pyautogui.press('enter')  # press the Enter key


    # self.__left_click_this_image(self.filename_formatted('process_button.png'))

    def __make_sure_video_is_added_successfully(self):
        self._left_click_this_coordinate(cancel_coordinates)
        time.sleep(0.1)
        self._left_click_this_coordinate(confirm_cancel_coordinates)
        time.sleep(0.5)
        pyautogui.press('esc')
        time.sleep(0.2)
        pyautogui.press('esc')

    def __find_and_close_unwanted_popup(self):
        # self.__left_click_this_image(self.filename_formatted("restore_pages_button.png"), False)
        pyautogui.hotkey('esc')
        pyautogui.hotkey('esc')

    def __open_browser(self):
        process_id = subprocess.Popen([self.browser_loc, self.case_url])
        time.sleep(60)
        self.__find_and_close_unwanted_popup()
        return process_id

    def __close_browser(self):
        if self.process == None:
            return
        self.process.kill()
        self.process=None

    def validate_browser(self):
        if self.__check_for_background_process() == False:
            self.clean_up()

    def go_to_main_screen(self):
        self._left_click_this_coordinate(back_coordinates)
        time.sleep(4)

    def prepare_browser(self,skip_login=False):
        if self.process == None:
            self.process = self.__open_browser()
            if self.process == None:
                logging_to_console_and_syslog("Process is None")
                raise BriefCamNoProcessExcept(self.process)
            if not skip_login:
                self.__login_to_briefcam_server()
            self.browser_ready = True

    def clean_up(self):
        self.browser_ready = False
        # close the browser and reopen it.
        logging_to_console_and_syslog("Closing and reopening web browser.")
        self.__close_browser()
        self.prepare_browser(skip_login=True)

    def __delete_video_clip_from_shared_volume(self, file_name):
        logging_to_console_and_syslog("Deleting file_name {}".format(file_name))
        os.remove(file_name)

    def process_new_file(self, file_name):
        job_done = False
        while job_done == False:
            try:
                while self.browser_ready == False:
                    logging_to_console_and_syslog("Waiting for the browser to be ready")
                    time.sleep(1)
                self.__search_and_leftclick_case(file_name)
                self.__add_video(file_name)
                self.__make_sure_video_is_added_successfully()
                self.__delete_video_clip_from_shared_volume(file_name)
                self.go_to_main_screen()
                self.validate_browser()
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
