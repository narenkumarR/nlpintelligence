from distutils.core import setup
from setuptools import  find_packages

#This is a list of files to install, and where
#(relative to the 'root' dir, where setup.py is)
#You could be more specific.
files = ["stanford-jars/*","text_processing/*","naive_bayes/*"]

setup(name = "intent_class_models",
    version = "0.0.1",
    description = "models for predicting intent class",
    author = "joswin",
    author_email = "joswin@ideas2it.com",
    # url = "whatever",
    #Name the folder where your packages live:
    #(If you have other packages (dirs) or modules (py files) then
    #put them into the package directory - they will be found
    #recursively.)
    packages = find_packages(),
      include_package_data=True,
    #'package' package must contain files (see list above)
    #I called the package 'package' thus cleverly confusing the whole issue...
    #This dict maps the package name =to=> directories
    #It says, package *needs* these files.
    package_data = {'' : files },
    #'runner' is in the root.
    # scripts = ["runner"],
    # long_description = """Really long text here."""
    #
    #This next part it for the Cheese Shop, look a little down the page.
    #classifiers = []
      zip_safe=False
)