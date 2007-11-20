from setuptools import setup, find_packages
import os, glob, sys

def list_my_packages() :
    retval = ['rocks/solid']
    for dir in os.listdir('rocks' + os.sep + 'solid') :
        path = os.path.join('rocks', 'solid', dir)
        if os.path.isdir(path) and (dir != '.svn') and (dir != 'CVS') :
            retval.append(os.path.join('rocks', 'solid', dir))
    return retval

setup(
    name = "rocks-solid",
    version = "0.1",
    #packages = find_packages(),
    packages = list_my_packages(),
    scripts = glob.glob('scripts/*-*'),

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = ['docutils >= 0.3'],

#    package_data = {
#        # If any package contains *.txt or *.rst files, include them:
#        '': ['*.txt', '*.rst'],
#        # And include any *.msg files found in the 'hello' package, too:
#        'hello': ['*.msg'],
#    },
    # metadata for upload to PyPI
    author = "Somsak Sriprayoonsakul",
    author_email = "somsaks@gmail.com",
    description = "Rocks-solid packages",
    license = "PSF",
    keywords = "cluster rocks",
    url = "http://code.google.com/hosting/p/rocks-solid",   # project home page, if any

    # could also include long_description, download_url, classifiers, etc.
)
