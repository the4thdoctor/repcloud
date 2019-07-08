#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup
from distutils.sysconfig import get_python_lib
#from os import listdir
#from os.path import isfile, join

def readme():
	try:
		with open('README.rst') as f:
			return f.read()
	except:
		return "Readme not available"

python_lib=get_python_lib()

package_data = ('%s/repcloud' % python_lib, ['LICENSE.txt'])

	

#sql_up_path = 'sql/upgrade'
conf_dir = "/%s/repcloud/config" % python_lib
sql_dir = "/%s/repcloud/sql" % python_lib
#sql_up_dir = "/%s/repcloud/%s" % (python_lib, sql_up_path)


data_files = []
conf_files = (conf_dir, ['config/config-example.toml'])

sql_src = ['sql/create_schema.sql', 'sql/drop_schema.sql']
#sql_upgrade = ["%s/%s" % (sql_up_path, file) for file in listdir(sql_up_path) if isfile(join(sql_up_path, file))]

sql_files = (sql_dir,sql_src)
#sql_up_files = (sql_up_dir,sql_upgrade)

data_files.append(conf_files)
data_files.append(sql_files)
#data_files.append(sql_up_files)



setup(
	name="repcloud",
	version="0.0.1dev",
	description="PostgreSQL repacker and transformer",
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
	data_files = data_files, 
	include_package_data = True, 
	python_requires='>=3.3',
	keywords='postgresql database cloud repack transform',
	
)
