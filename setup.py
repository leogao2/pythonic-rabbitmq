
from distutils.core import setup

from setuptools import find_packages

setup(name='pythonic-rabbitmq',
      version='0.1.0',
      description='pythonic interface to rabbitmq',
      author='Leo Gao',
      author_email='leogao31@gmail.com',
      url='https://github.com/leogao2/pythonic-rabbitmq',
      packages=find_packages(),
      install_requires=[
          'pika',
          'cloudpickle'
      ]
)
