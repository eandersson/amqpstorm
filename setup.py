from setuptools import setup


setup(name='amqpstorm',
      version='1.0.0',
      description='Python AMQP Client Library',
      long_description='',
      maintainer='Erik Olof Gunnar Andersson',
      include_package_data=True,
      packages=['amqpstorm'],
      install_requires=['pamqp'],
      package_data={'': ['README.md']},
      zip_safe=False,
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Communications',
          'Topic :: Internet',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: System :: Networking'])