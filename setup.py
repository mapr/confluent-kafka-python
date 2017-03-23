#!/usr/bin/env python

from setuptools import setup, find_packages
from distutils.core import Extension


module = Extension('mapr_streams_python.cimpl',
                    libraries= ['rdkafka'],
                    sources=['mapr_streams_python/src/mapr_streams_python.c',
                             'mapr_streams_python/src/Producer.c',
                             'mapr_streams_python/src/Consumer.c'])

setup(name='mapr-streams-python',
      version='0.9.2',
      description='Confluent\'s Apache Kafka client for Python',
      author='Confluent Inc',
      author_email='support@confluent.io',
      url='https://github.com/confluentinc/confluent-kafka-python',
      ext_modules=[module],
      packages=find_packages(),
      data_files = [('', ['LICENSE'])])
