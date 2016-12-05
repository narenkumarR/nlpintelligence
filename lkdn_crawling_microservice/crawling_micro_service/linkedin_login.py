__author__ = 'joswin'

import time
import logging
import random
from constants import linkedin_dets

def login_fun(browser):
    '''
    :return:
    '''
    try:
        linkedin_username,linkedin_password = random.choice(linkedin_dets)
        browser.get('https://www.linkedin.com/')
        time.sleep(10)
        username_field = browser.find_element_by_id("login-email")
        password_field = browser.find_element_by_id("login-password")
        username_field.send_keys(linkedin_username)
        password_field.send_keys(linkedin_password)
        browser.find_element_by_name("submit").click()
        time.sleep(30)
    except:
        logging.exception('Login failed. continue without logging in')
        pass