#not working properly with soup
#http://stackoverflow.com/questions/17140039/linkedin-automatic-login-using-python
#http://www.pythonforbeginners.com/cheatsheet/python-mechanize-cheat-sheet/
#
# import mechanize
#
# browser = mechanize.Browser()
# browser.set_handle_robots(False)
# browser.open("https://www.linkedin.com/")
# ## browser.select_form(name="login")
# browser.form = list(browser.forms())[0]
#
# browser["session_key"] = "your_email"
# browser["session_password"] = "your_password"
# response = browser.submit()
#
# print response.read()


https://www.linkedin.com/vsearch/p?title=ceo%20OR%20coo&company=zendrive%20OR%20hackerrank%20OR%20hackerearth