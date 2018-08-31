import pyautogui
import os
import logging
import subprocess
import time
import sys


def filename_formatted(filename):
    if __name__ == "__main__":
        return filename
    else:
        return os.path.dirname(os.path.realpath(__file__)) + "/" + self.image_directory + "/" + filename

class upload_video_to_briefcam():
    pyautogui.PAUSE = 0.25
    unwanted_popup = filename_formatted("restore_pages_button.png")

    def __init__(self):
        self.video_filename=None
        self.case_name=None
        self.case_url=None
        self.browser_loc=None
        self.username=None
        self.password=None
        self.image_directory=None
        self.import_environment_variables()

    def import_environment_variables(self):
        while self.case_name==None:
            time.sleep(2)
            logging.debug("Trying to read the environment variables")
            self.case_name = os.getenv("case_name_key", default=None)
            self.case_url = os.getenv("case_url_key", default=None)
            self.browser_loc = os.getenv("browser_loc_key", default=None)
            self.username = os.getenv("login_username_key", default=None)
            self.password = os.getenv("login_password_key", default=None)
            self.image_directory = os.getenv("image_directory", default=None)
        logging.debug("password={}".format(self.password))
        logging.debug("case_name={}".format(self.case_name))
        logging.debug("case_url={}".format(self.case_url))
        logging.debug("username={}".format(self.username))
        logging.debug("browser_loc={}".format(self.browser_loc))
        logging.debug("image_directory={}".format(self.image_directory))

    def __proceed_with_execution(self):
        # pyautogui.alert('Shall I proceed in creating a case?')
        text = pyautogui.confirm(text='Shall I proceed in creating a case?', title='Question', buttons=['OK', 'Cancel'])
        if text == 'Cancel':
            return False
        return True

    def __left_click_this_image(self, button_name, force_wait=True):
        button_location = None
        while button_location == None:
            logging.debug("Trying to match " + button_name)
            try:
                button_location = pyautogui.locateOnScreen(button_name, grayscale=False)
            except FileNotFoundError:
                logging.debug("File Not Found")
                break
        if button_location == None and force_wait == False:
            return False
        logging.debug("button_name={},location={}".format(button_name, button_location))
        buttonx, buttony = pyautogui.center(button_location)
        logging.debug("buttonx={} and buttony={}".format(buttonx, buttony))
        pyautogui.click(buttonx, buttony)
        return True

    def __login_to_briefcam_server(self):
        pyautogui.press('esc')
        self.__left_click_this_image(filename_formatted('sandbox_stability.png'), False)
        return_value = self.__left_click_this_image(filename_formatted('signin_button.png'), False)
        if return_value == True:
            pyautogui.hotkey('tab')
            pyautogui.typewrite(self.username, interval=0.25)
            pyautogui.hotkey('tab')
            pyautogui.typewrite(self.password, interval=0.25)
            pyautogui.press('enter')  # press the Enter key
            pyautogui.press('esc')

    defBrief__create_case(self):
        return_value =None
        return_value =self.__left_click_this_image(filename_formatted('mec_poc_button.png'), False)
        if return_value == False:
            # MEC-POC case is getting created for the first time.
            self.__left_click_this_image(filename_formatted('create_case_button.png'))
            pyautogui.typewrite(self.case_name, interval=0.25) # prints out the case name with a quarter second delay after each character
            pyautogui.hotkey('tab')
            pyautogui.hotkey('tab')
            pyautogui.hotkey('tab')
            pyautogui.hotkey('tab')
            pyautogui.hotkey('tab')
            # left_click_this_image('create_button.png')
            pyautogui.press('enter')  # press the Enter key
            self.__left_click_this_image(filename_formatted('mec_poc_button.png'))

    def __add_video(self, file_name):///
        self.__left_click_this_image(filename_formatted('add_video_to_case2_button.png'))
        self.__left_click_this_image(filename_formatted('same_camera_button.png'), False)
        self.__left_click_this_image(filename_formatted('next_button.png'))
        self.__left_click_this_image(filename_formatted('browse_button.png'))
        pyautogui.typewrite(file_name, interval=0.25)
        pyautogui.press('enter')  # press the Enter key
        # left_click_this_image('open_button.png')
        self.__left_click_this_image(filename_formatted('next2_button.png'))
        self.__left_click_this_image(filename_formatted('process_button.png'))

    def __find_and_close_unwanted_popup(self):
        self.__left_click_this_image(upload_video_to_briefcam.unwanted_popup, False)
        pyautogui.hotkey('esc')
        pyautogui.hotkey('esc')

    def __open_browser(self):
        process_id =subprocess.Popen([self.browser_loc, self.case_url])
        time.sleep(2)
        self.__find_and_close_unwanted_popup()
        return process_id

    def __close_browser(self, process):
        if process==None:
            return
        process.kill()

    def process_new_file(self,file_name):
        process =self.__open_browser()
        if process==None:
            logging.debug("Process is None")
            raise NoProcessExcept(process)
        self.__login_to_briefcam_server()
        self.__create_case()
        self.__add_video(file_name)
        self.__close_browser(process)

if __name__=="__main__":
    #define your unit test cases here.
    #obj=upload_video_to_briefcam()
    #obj.process_new_file("test")
    pass
