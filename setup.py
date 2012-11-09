from distutils.core import setup
setup(
    name='quick_clone',
    version='0.0.1',
    packages=[
        'quick_clone'
    ],
    scripts=[
        'bin/quick_clone'
    ],
    author='Jed Galbraith',
    author_email='jed@delorum.com',
    # license='LICENSE.txt',
    description='quick clone db',
    requires=[
        "argparse",
        "mysql"
    ]
)