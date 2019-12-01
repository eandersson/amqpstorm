from setuptools import find_packages
from setuptools import setup

setup(
    name='AMQPStorm',
    version='2.7.2',
    description='Thread-safe Python RabbitMQ Client & Management library.',
    long_description=open('README.rst').read(),
    author='Erik Olof Gunnar Andersson',
    author_email='me@eandersson.net',
    include_package_data=True,
    packages=find_packages(),
    license='MIT License',
    url='https://www.amqpstorm.io',
    install_requires=['pamqp>=2.0.0,<3.0'],
    extras_require={
        'management': ['requests']
    },
    package_data={'': ['README.rst', 'LICENSE', 'CHANGELOG.rst']},
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
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
