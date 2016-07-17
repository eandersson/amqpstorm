from setuptools import setup

long_description = """
See https://github.com/eandersson/amqpstorm for more information.
"""

setup(name='AMQPStorm',
      version='1.5.0',
      description='Thread-safe Python RabbitMQ Client Library based on pamqp.',
      long_description=long_description,
      author='Erik Olof Gunnar Andersson',
      author_email='me@eandersson.net',
      include_package_data=True,
      packages=['amqpstorm', 'amqpstorm.tests',
                'amqpstorm.tests.functional',
                'amqpstorm.tests.unit'],
      license='MIT License',
      url='http://github.com/eandersson/amqpstorm',
      install_requires=['pamqp>=1.6.1,<2.0'],
      package_data={'': ['README.rst', 'LICENSE', 'CHANGELOG.rst']},
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: Implementation :: CPython',
          'Programming Language :: Python :: Implementation :: PyPy',
          'Topic :: Communications',
          'Topic :: Internet',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: System :: Networking'])
