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
        time.sleep(10)
        try:
            submit = browser.find_element_by_id("login-submit")
            submit.click()
        except:
            try:
                submit = browser.find_element_by_class_name("login submit-button")
                submit.click()
            except:
                try:
                    submit = browser.find_element_by_name("submit")
                    submit.click()
                except:
                    submit = browser.find_element_by_link_text("Sign in")
                    submit.click()
        time.sleep(50)
    except:
        logging.exception('Login failed. continue without logging in')
        pass