from setuptools import setup

setup(name='dbmanager',
      version='1.0',
      description='MySQL Data I/O with Python',
      keywords='mysql, database, data flow',
      url='https://github.com/jmmanso/dbmanager',
      author='Jesus Martinez-Manso',
      author_email='j.martinez.manso@gmail.com',
      install_requires = ['numpy', 'MySQL-python'],
      packages=['dbmanager'],
      zip_safe=False)
