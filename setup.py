#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, setuptools
from distutils.sysconfig import get_python_lib

def readme():
	try:
		with open('README.rst') as f:
			return f.read()
	except:
		return "Readme not available"

python_lib=get_python_lib()

package_data = {'repcloud': ['config/*.toml', 'sql/*.sql', 'LICENSE.txt']}


setup(
	name="repcloud",
	version="0.1.a1",
	description="PostgreSQL repacker without need",
	long_description=readme(),
	author = "Federico Campoli",
	author_email = "the4thdoctor.gallifrey@gmail.com",
	maintainer = "Federico Campoli",
	maintainer_email = "the4thdoctor.gallifrey@gmail.com",
	url="https://github.com/the4thdoctor/repcloud/",
	license="BSD License",
	platforms=[
		"linux"
	],
	classifiers=[
		"License :: OSI Approved :: BSD License",
		"Environment :: Console",
		"Intended Audience :: Developers",
		"Intended Audience :: Information Technology",
		"Intended Audience :: System Administrators",
		"Natural Language :: English",
		"Operating System :: POSIX :: BSD",
		"Operating System :: POSIX :: Linux",
		"Programming Language :: Python",
		"Programming Language :: Python :: 3",
		"Programming Language :: Python :: 3.3",
		"Programming Language :: Python :: 3.4",
		"Programming Language :: Python :: 3.5",
		"Programming Language :: Python :: 3.6",
		"Topic :: Database :: Database Engines/Servers",
		"Topic :: Other/Nonlisted Topic"
	],
	py_modules=[
		"repcloud.__init__",
		"repcloud.repack_lib",
		"repcloud.pg_lib",
	],
	scripts=[
		"scripts/rpcl.py",
		"scripts/rpcl"
	],
	install_requires=[
		'argparse>=1.2.1',
		'psycopg2-binary>=2.7.4',
		'toml>=0.10.0',
		'tabulate>=0.8.1',
		'daemonize>=2.4.7',
		'sphinx>=2.1.2',
	],
	package_data=package_data,
	packages=setuptools.find_packages(),
	include_package_data = True,
	python_requires='>=3.5',
	keywords='postgresql database cloud repack transform',

)
