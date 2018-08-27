import pyautogui
import os
import logging

pyautogui.PAUSE = 0.25

def proceed_with_execution():
    #pyautogui.alert('Shall I proceed in creating a case?')
    text=pyautogui.confirm(text='Shall I proceed in creating a case?', title='Question', buttons=['OK', 'Cancel'])
    if text == 'Cancel':
        return False
    return True

def left_click_this_image(button_name ,force_wait=True):
    button_location =None
    while button_location == None:
        logging.debug("Trying to match " + button_name)
        button_location = pyautogui.locateOnScreen(button_name ,grayscale=False)
        if button_location==None:
            button_name2 =button_name[:button_name.find('.')] + '2' + '.png'
            logging.debug("Trying to match " + button_name2)
            button_location = pyautogui.locateOnScreen(button_name2 ,grayscale=False)
        if button_location==None:
            try:
                button_name2 =button_name[:button_name.find('.')] + '3' + '.png'
                logging.debug("Trying to match " + button_name2)
                button_location = pyautogui.locateOnScreen(button_name2 ,grayscale=False)
            except FileNotFoundError:
                logging.debug("File Not Found")
        if button_location==None and force_wait==False:
            return False
    logging.debug("button_name={},location={}".format(button_name ,button_location))
    buttonx, buttony = pyautogui.center(button_location)
    logging.debug("buttonx={} and buttony={}".format(buttonx ,buttony))
    pyautogui.click(buttonx, buttony)
    return True

def login_to_briefcam_server():
    username="Brief"
    password="Cam"
    return_value = left_click_this_image('signin_button.png' ,False)
    if return_value == True:
        pyautogui.hotkey('tab')
        pyautogui.typewrite(username, interval=0.25)
        pyautogui.hotkey('tab')
        pyautogui.typewrite(password, interval=0.25)
        pyautogui.press('enter')  # press the Enter key

case = os.getenv("case_name_key", default=None)
logging.debug("case={}".format(case))

def create_case(case_name=case):
    return_value =None
    return_value =left_click_this_image('mec_poc_button.png' ,False)
    if return_value == False:
        # MEC-POC case is getting created for the first time.
        left_click_this_image('create_case_button.png')
        pyautogui.typewrite(case_name, interval=0.25) # prints out the case name with a quarter second delay after each character
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        pyautogui.hotkey('tab')
        # left_click_this_image('create_button.png')
        pyautogui.press('enter')  # press the Enter key
        left_click_this_image('mec_poc_button.png')

def add_video(file_name):
    left_click_this_image('add_video_to_case2_button.png')
    left_click_this_image('same_camera_button.png' ,False)
    left_click_this_image('next_button.png')
    left_click_this_image('browse_button.png')
    pyautogui.typewrite(file_name, interval=0.25)
    pyautogui.press('enter')  # press the Enter key
    # left_click_this_image('open_button.png')
    left_click_this_image('next2_button.png')
    left_click_this_image('process_button.png')

import subprocess
import time

case_url = os.getenv("case_url_key", default=None)
logging.debug("case_url={}".format(case_url))

browser_loc = os.getenv("browser_loc_key", default=None)
logging.debug("browser_loc={}".format(browser_loc))

unwanted_popup ="restore_pages_button.png"
def find_and_close_unwanted_popup(popup=unwanted_popup):
    left_click_this_image(unwanted_popup ,False)
    pyautogui.hotkey('esc')
    pyautogui.hotkey('esc')

def open_browser(url=case_url):
    process_id =subprocess.Popen([browser_loc, case_url])
    time.sleep(2)
    find_and_close_unwanted_popup()
    return process_id

def close_browser(process):
    if process==None:
        return
    process.kill()

def process_new_file(file_name):
    process =open_browser()
    if process==None:
        logging.debug("Process is None")
        raise NoProcessExcept(process)
    login_to_briefcam_server()
    create_case()
    add_video(file_name)
    close_browser(process)
