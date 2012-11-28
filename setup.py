# from distutils.core import setup
# setup(
from setuptools import setup, find_packages
setup(
    name='quick_clone',
    version='0.0.1',
    packages=[
        'quick_clone',
        # find_packages(),
    ],
    scripts=[
        'bin/quick_clone'
    ],
    author='Jed Galbraith',
    author_email='jed@delorum.com',
    # license='LICENSE.txt',
    description='Quickly clone database via MySQL INTO OUTFILE & LOAD DATA INFILE commands',
    requires=[
        "argparse",
        "mysql"
    ]
)