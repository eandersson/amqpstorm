from setuptools import setup

setup(
    name='AMQPStorm',
    version='2.1.0',
    description='Thread-safe Python RabbitMQ Client & Management library.',
    long_description=open('README.rst').read(),
    author='Erik Olof Gunnar Andersson',
    author_email='me@eandersson.net',
    include_package_data=True,
    packages=[
        'amqpstorm',
        'amqpstorm.management',
        'amqpstorm.tests',
        'amqpstorm.tests.functional',
        'amqpstorm.tests.functional.management',
        'amqpstorm.tests.unit',
        'amqpstorm.tests.unit.management'
    ],
    license='MIT License',
    url='http://github.com/eandersson/amqpstorm',
    install_requires=['pamqp>=1.6.1,<2.0'],
    extra_require={
        'management_api': ['requests']
    },
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
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking'
    ]
)
