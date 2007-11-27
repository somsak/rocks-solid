#from setuptools import setup, find_packages
import os, glob, sys
from distutils.core import setup

def list_my_packages() :
    retval = ['rocks_solid']
    for dir in os.listdir('rocks_solid') :
        path = os.path.join('rocks_solid', dir)
        if os.path.isdir(path) and (dir != '.svn') and (dir != 'CVS') :
            retval.append(os.path.join('rocks_solid', dir))
    return retval

#    entry_points = {
#        'console_scripts': [
#            'cluster-ipmi = rocks_solid.app:run_cluster_ipmi',
#            'cluster-power = rocks_solid.app:run_cluster_power',
#            'node-cleanipcs = rocks_solid.app:run_node_cleanipcs',
#            'node-term-user-ps = rocks_solid.app:run_node_term_user_ps',
#            'node-term-sge-zombie = rocks_solid.app:run_node_term_sge_zombie',
#            'cluster-freehost = rocks_solid.app:run_cluster_freehost', 
#            'cluster-clean-ps = rocks_solid.app:run_cluster_clean_ps',
#        ]
#    },
entry_points = [
    'cluster-ipmi = rocks_solid.app:run_cluster_ipmi',
    'cluster-power = rocks_solid.app:run_cluster_power',
    'node-cleanipcs = rocks_solid.app:run_node_cleanipcs',
    'node-term-user-ps = rocks_solid.app:run_node_term_user_ps',
    'node-term-sge-zombie = rocks_solid.app:run_node_term_sge_zombie',
    'cluster-freehost = rocks_solid.app:run_cluster_freehost', 
    'cluster-clean-ps = rocks_solid.app:run_cluster_clean_ps',
]

scripts = []

def list_my_scripts() :
    if not os.path.exists('scripts') :
        os.mkdir('scripts')
    for script in entry_points :
        script, module = script.split('=')
        script = script.strip()
        mod, func = module.strip().split(':')
        path = os.path.join('scripts', script)
        f = open(path, 'w')
        f.write('''#!%s
import %s
%s.%s()
''' % (sys.executable, mod, mod, func) ) 
        f.close()
        scripts.append(path)
    return scripts

setup(
    name = "rocks-solid",
    version = "0.1",
#    packages = find_packages(),
    packages = list_my_packages(),
#    scripts = glob.glob('scripts/*-*'),
    scripts = list_my_scripts(),

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
#    install_requires = ['docutils >= 0.3'],

#    package_data = {
#        # If any package contains *.txt or *.rst files, include them:
#        '': ['*.txt', '*.rst'],
#        # And include any *.msg files found in the 'hello' package, too:
#        'hello': ['*.msg'],
#    },
    # metadata for upload to PyPI
#    entry_points = {
#        'console_scripts': [
#            'cluster-ipmi = rocks_solid.app:run_cluster_ipmi',
#            'cluster-power = rocks_solid.app:run_cluster_power',
#            'node-cleanipcs = rocks_solid.app:run_node_cleanipcs',
#            'node-term-user-ps = rocks_solid.app:run_node_term_user_ps',
#            'node-term-sge-zombie = rocks_solid.app:run_node_term_sge_zombie',
#            'cluster-freehost = rocks_solid.app:run_cluster_freehost', 
#            'cluster-clean-ps = rocks_solid.app:run_cluster_clean_ps',
#        ]
#    },
    author = "Somsak Sriprayoonsakul",
    author_email = "somsaks@gmail.com",
    description = "Rocks-solid packages",
    license = "PSF",
    keywords = "cluster rocks",
    url = "http://code.google.com/p/rocks-solid",   # project home page, if any

    # could also include long_description, download_url, classifiers, etc.
)
