import sys
import os, os.path
from shutil import copy
from distutils.sysconfig import get_python_lib
import toml
from tabulate import tabulate
from repcloud import pg_engine
class repack_engine():
	def __init__(self,  args):
		"""
		This is the repack engine class constructor.
		The method calls the private method __set_configuration_files()
		"""
		self.args = args
		app_dir = "repcloud"
		config_dir = "config"
		self.catalog_version = '0.0.1'
		self.lst_yes =  ['yes',  'Yes', 'y', 'Y']
		config_file = self.args.config.split('.')
		python_lib=get_python_lib()
		self.connection = None

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
		self.__set_configuration_files()
		self.__set_conf_permissions(rep_dir)

		if config_file [-1]=='toml':
			self.config_file = "%s/%s" % (local_conf,  self.args.config )
		else:
			self.config_file = "%s/%s.toml" % (local_conf,  self.args.config )
		
		self.__load_config()

		self.pg_engine = pg_engine()

	def __load_config(self):
		"""
		The method loads the configuration from the file specified in the args.config parameter.
		"""
		if not os.path.isfile(self.config_file):
			print("**FATAL - configuration file missing. Please ensure the file %s is present." % (self.config_file))
			sys.exit()

		config_file = open(self.config_file, 'r')
		self.config = toml.loads(config_file.read())
		config_file.close()

	def __check_connections(self, connection=None):
		"""
		The method runs safety check on the connection set in the configuration file and the
		connection passed as parameter.
		If the checks are passed then the class variable self.connection is set to the connections dictionary for further usage.
		"""
		if 'connections' not in self.config:
			print("There is no connection defined in the configuration file %s" % self.config_file)
			sys.exit(1)
		if self.args.connection !="all":
			if self.args.connection not in self.config['connections']:
				print("You specified a not existent connection.")
				sys.exit(2)
		self.connection = self.config["connections"]

	def show_connections(self):
		"""
		Displays the connections available for the configuration  nicely formatted
		"""
		self.__check_connections()
		for item in self.config["connections"]:
			print (tabulate([], headers=["Connection %s" % item]))
			tab_headers = ['Parameter', 'Value']
			tab_body = []
			connection = self.config["connections"][item]
			connection_list = [param for param  in connection if param not in ['password']]
			for parameter in connection_list:
				tab_row = [parameter, connection[parameter]]
				tab_body.append(tab_row)
			print(tabulate(tab_body, headers=tab_headers))

	def create_schema(self):
		"""
		The method creates the repack schema for the target connection.
		"""
		self.__check_connections()
		self.pg_engine.create_repack_schema(self.connection, self.args.connection )

	def drop_schema(self):
		"""
		The method drops the repack schema for the target connection.
		"""
		self.__check_connections()
		self.pg_engine.drop_repack_schema(self.connection, self.args.connection )

	def __set_conf_permissions(self,  rep_dir):
		"""
			The method sets the permissions of the configuration directory to 700
			
			:param rep_dir: the configuration directory 
		"""
		if os.path.isdir(rep_dir):
			os.chmod(rep_dir, 0o700)


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
