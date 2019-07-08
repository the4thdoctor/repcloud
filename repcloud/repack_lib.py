import os, os.path
import sys
import time
import signal
from shutil import copy
from distutils.sysconfig import get_python_lib

class repack_engine():
	def __init__(self,  args):
		"""
			This is the repack engine class constructor.
			The method calls the private method __set_configuration_files() 
		"""
		app_dir = "repcloud"
		config_dir = "config"
		self.catalog_version = '0.0.1'
		self.lst_yes =  ['yes',  'Yes', 'y', 'Y']
		python_lib=get_python_lib()
		rep_dir = "%s/.%s" % (os.path.expanduser('~'),  app_dir)
		
			
		local_conf = "%s/%s" % ( rep_dir, config_dir )
		self.global_conf_example = '%s/%s/%s/config-example.toml' % (python_lib,  app_dir, config_dir, )
		self.local_conf_example = '%s/config-example.yml' % local_conf
		
		local_logs = "%s/logs/" % rep_dir
		local_pid = "%s/pid/" % rep_dir
		
		self.conf_dirs=[
			rep_dir, 
			local_conf, 
			local_logs, 
			local_pid, 
		]
		print (self.conf_dirs)
		self.__set_configuration_files()
		self.args = args
		
	def __set_configuration_files(self):
		""" 
			The method loops the list self.conf_dirs creating them only if they are missing.
			
			The method checks the freshness of the config-example.yaml and connection-example.toml 
			copies the new version from the python library determined in the class constructor with get_python_lib().
			
			If the configuration file is missing the method copies the file with a different message.
		
		"""

		for confdir in self.conf_dirs:
			if not os.path.isdir(confdir):
				print ("creating directory %s" % confdir)
				os.mkdir(confdir)
		
				
		if os.path.isfile(self.local_conf_example):
			if os.path.getctime(self.global_conf_example)>os.path.getctime(self.local_conf_example):
				print ("updating configuration example with %s" % self.local_conf_example)
				copy(self.global_conf_example, self.local_conf_example)
		else:
			print ("copying configuration  example in %s" % self.local_conf_example)
			copy(self.global_conf_example, self.local_conf_example)
