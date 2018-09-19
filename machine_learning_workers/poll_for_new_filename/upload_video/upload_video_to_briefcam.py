import pyautogui
import os
import subprocess
import time
import sys
sys.path.append("..") # Adds higher directory to python modules path.

from log.log_file import logging_to_console_and_syslog

search_coordinates = (840, 200)
case_coordinates = (83, 565)

class TimeOutException(Exception):
    def __init__(self):
        logging_to_console_and_syslog("Time out exception occured")


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
        self.import_environment_variables()
        self.prepare_browser()

    def import_environment_variables(self):
        while self.case_name == None:
            time.sleep(2)
            logging_to_console_and_syslog("Trying to read the environment variables")
            self.case_name = os.getenv("case_name_key", default=None)
            self.case_url = os.getenv("case_url_key", default=None)
            self.browser_loc = os.getenv("browser_loc_key", default=None)
            self.username = os.getenv("login_username_key", default=None)
            self.password = os.getenv("login_password_key", default=None)
            self.image_directory = os.getenv("image_directory", default=None)
        logging_to_console_and_syslog("password={}".format(self.password))
        logging_to_console_and_syslog("case_name={}".format(self.case_name))
        logging_to_console_and_syslog("case_url={}".format(self.case_url))
        logging_to_console_and_syslog("username={}".format(self.username))
        logging_to_console_and_syslog("browser_loc={}".format(self.browser_loc))
        logging_to_console_and_syslog("image_directory={}".format(self.image_directory))

    def __proceed_with_execution(self):
        # pyautogui.alert('Shall I proceed in creating a case?')
        text = pyautogui.confirm(text='Shall I proceed in creating a case?', title='Question', buttons=['OK', 'Cancel'])
        if text == 'Cancel':
            return False
        return True

    max_retries = 10

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
                    raise TimeOutException
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
            time.sleep(0.25)
            pyautogui.typewrite(self.username, interval=0.1)
            pyautogui.hotkey('tab')
            time.sleep(0.25)
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

        logging_to_console_and_syslog("Found casename {}.".format(self.case_name))

    def __create_case_for_the_first_time(self):
        # MEC-POC case is getting created for the first time.
        self.__left_click_this_image(self.filename_formatted('create_case_button.png'))
        time.sleep(0.25)
        pyautogui.typewrite(self.case_name,
                            interval=0.1)  # prints out the case name with a quarter second delay after each character
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.press('enter')  # press the Enter key
        time.sleep(0.25)
        self._left_click_this_coordinate(case_coordinates)

    def __create_case(self,filename):
        no_results_found = None
        self.__extract_case_name_from_video_file_name(filename)
        self._left_click_this_coordinate(search_coordinates)
        time.sleep(0.25)
        pyautogui.typewrite(self.case_name, interval=0.1)
        logging_to_console_and_syslog("Clicking on case at location:{}".format(case_coordinates))
        self._left_click_this_coordinate(case_coordinates)

    def __check_for_add_video_button(self):
        return_value = False
        for index in range(5):
            return_value = self.__left_click_this_image(self.filename_formatted('add_video_to_case_button.png'), False)
            if return_value:
                break
        return return_value

    def __add_video(self, file_name):
        add_video_button_found=self.__check_for_add_video_button()
        if not add_video_button_found:
            logging_to_console_and_syslog("Add video button is not found. Creating the casename. {}."
                                          .format(self.case_name))
            self.__create_case_for_the_first_time()
                if self.__check_for_add_video_button() is False:
                logging_to_console_and_syslog("Add video button is still not found.")
                raise TimeOutException
        else:
            self.__left_click_this_image(self.filename_formatted('same_camera_button.png'), False)
            self.__left_click_this_image(self.filename_formatted('next_button.png'))
            self.__left_click_this_image(self.filename_formatted('browse_button.png'))
            time.sleep(0.25)
            pyautogui.typewrite(file_name, interval=0.1)
            pyautogui.press('enter')  # press the Enter key
            # left_click_this_image('open_button.png')
            self.__left_click_this_image(self.filename_formatted('next2_button.png'))
            # pyautogui.hotkey('tab')
            pyautogui.press('enter')  # press the Enter key

    # self.__left_click_this_image(self.filename_formatted('process_button.png'))

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

    def prepare_browser(self):
        if self.process == None:
            self.process = self.__open_browser()
            if self.process == None:
                logging_to_console_and_syslog("Process is None")
                raise NoProcessExcept(self.process)
            self.__login_to_briefcam_server()
            self.browser_ready = True

    def process_new_file(self, file_name):
        job_done = False
        while job_done == False:
            try:
                while self.browser_ready == False:
                    logging_to_console_and_syslog("Waiting for the browser to be ready")
                    time.sleep(1)
                self.__create_case(file_name)
                self.__add_video(file_name)
                job_done = True
            except TimeOutException:
                # close the browser and reopen it.
                logging_to_console_and_syslog("Closing and reopening web browser.")
                self.__close_browser()
                self.prepare_browser()
# self.__close_browser(process)


if __name__ == "__main__":
    # define your unit test cases here.
    # obj=UploadVideoToBriefCam()
    # obj.process_new_file("test")
    # Brieobj.filename_formatted("test")
    pass
