#!/usr/bin/env python
from setuptools import setup, find_packages

with open('./README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(name='mariadb',
      version="2.0.0",
      python_requires='>=3.6',
      classifiers = [
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Environment :: MacOS X',
          'Environment :: Win32 (MS Windows)',
          'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
          'Programming Language :: C',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: MacOS',
          'Operating System :: POSIX',
          'Intended Audience :: End Users/Desktop',
          'Intended Audience :: Developers',
          'Intended Audience :: System Administrators',
          'Topic :: Database'
      ],
      description='Python MariaDB extension',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Georg Richter,Diego Dupin',
      license='LGPL 2.1',
      url='https://www.github.com/mariadb-corporation/mariadb-connector-python',
      project_urls={
         "Bug Tracker": "https://jira.mariadb.org/",
         "Documentation": "https://mariadb-corporation.github.io/mariadb-connector-python/",
         "Source Code": "https://www.github.com/mariadb-corporation/mariadb-connector-python",
      },
      packages=find_packages(exclude=["tests*"])
)