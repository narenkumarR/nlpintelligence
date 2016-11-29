import getpass
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys 
from selenium.webdriver.support.ui import WebDriverWait

class LinkedInLogin(object):

    def __init__(self, login, password):

        url = "https://www.linkedin.com/uas/login"
        driver = webdriver.Firefox()
        driver.get(url)
        self.login = login
        self.password = password
        # username = 'gokul.contractiq@gmail.com'
        # password = 'gokulkrish007'
        user = driver.find_element_by_name("session_key")
        for j in login:
            user.send_keys(j)
        pasw = driver.find_element_by_name("session_password")
        for j in password:
            pasw.send_keys(j)
        driver.find_element_by_css_selector("div.form-buttons>input").click()
        url_next = "https://www.google.co.in/url?sa=t&rct=j&q=&esrc=s&source=web&cd=29&cad=rja&uact=8&ved=0ahUKEwjL5NvTkZbQAhWJqI8KHY8XBU0Q9zAIqgEwHA&url=https%3A%2F%2Fwww.linkedin.com%2Fcompany%2Fnewegg-com&usg=AFQjCNGkO2TpZKn--ekdkhRlWbfgyXthQQ&bvm=bv.137904068,d.c2I"
        driver.get(url_next)
        driver.close()

# driver.find_element_by_link_text("Edit Profile").click()

parser = LinkedInLogin('gokul.contractiq@gmail.com', 'gokulkrish007')
