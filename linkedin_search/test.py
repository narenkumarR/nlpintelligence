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


# https://www.linkedin.com/vsearch/p?title=ceo%20OR%20coo&company=zendrive%20OR%20hackerrank%20OR%20hackerearth
import search_linkedin
results_df = search_linkedin.LinkedinSearcher().get_all_results({'title':['ceo','coo'],'company':['zendrive','hackerrank','headout','razorpay','mustseeindia','springboard']})

# https://www.linkedin.com/vsearch/p?title=ceo%20OR%20coo&company=zendrive%20OR%20hackerrank%20OR%20hackerearth%20OR%20ideas2it&openAdvancedForm=true&titleScope=CP&companyScope=CP&locationType=Y&rsid=3443631031458731807410&orig=ADVS
# https://www.linkedin.com/vsearch/p?title=ceo%20OR%20coo&company=zendrive%20OR%20hackerrank%20OR%20hackerearth%20OR%20ideas2it&openAdvancedForm=true&titleScope=C&companyScope=C&locationType=Y&rsid=3443631031458731869694&orig=MDYS