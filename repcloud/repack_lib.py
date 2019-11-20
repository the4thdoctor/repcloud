import sys
import os, os.path
from shutil import copy
import toml
from tabulate import tabulate
from repcloud import pg_engine
from daemonize import Daemonize
import logging
from logging.handlers  import TimedRotatingFileHandler
import smtplib, ssl
from email.mime import text,multipart
import signal

class rep_notifier():
	def __init__(self, args):
		"""
		Class constructor for the notifier facility
		"""
		self.args = args
	def __init_emailer(self):
		"""
			The method initialise the emailer if any configuration is present
		"""
		if "email" in self.args:
			self.logger.log_message('Initialising emailer' , 'info')
			emailconf = self.args["email"]
			self.emailer = smtplib.SMTP(host=emailconf["smtp_server"], port=emailconf["smtp_port"])
			if emailconf["smtp_ssl"] == "starttls":
				context = ssl.create_default_context()
				self.emailer.starttls(context=context )

	def send_notification(self, subject, message):
		"""
			The method sends the notification according with which notifier is enabled and configured
		"""
		if self.args["enable_email"]:
			self.__send_email(subject, message)

	def __send_email(self, subject, message):
		"""
			The method sends the email with the given subject to the list of emails configured in notifier.email
		"""
		if "email" in self.args:
			self.__init_emailer()
			emailconf = self.args["email"]
			self.emailer.login(emailconf["smtp_username"], emailconf["smtp_password"])
			msg_plain = text.MIMEText(message, 'plain')
			for mailto in emailconf["mailto"]:
				self.logger.log_message('Sending email to %s' %(mailto, ) , 'info')
				msg_m = multipart.MIMEMultipart()
				msg_m['From'] = emailconf["mailfrom"]
				msg_m['To'] = mailto
				msg_m['Subject'] = subject
				msg_m.attach(msg_plain)
				send_msg = msg_m.as_string()
				self.emailer.sendmail(emailconf["mailfrom"], mailto, send_msg)
			self.emailer.quit()

class rep_logger():
	def __init__(self, args):
		"""
			Class constructor for the logging facility
		"""
		self.args = args
		self.__init_logger()


	def __log_file(self, message, level):
		"""
		The method logs on file the message on file according with the log level
		"""
		if level =='info':
			self.file_logger.info(message)
		elif level =='debug':
			self.file_logger.debug(message)
		elif level =='warning':
			self.file_logger.warning(message)
		elif level =='error':
			self.file_logger.error(message)
		elif level =='critical':
			self.file_logger.critical(message)

	def __log_console(self, message, level):
		"""
			The method logs on file the message on the console according with the log level
		"""
		if level =='info':
			self.cons_logger.info(message)
		elif level =='debug':
			self.cons_logger.debug(message)
		elif level =='warning':
			self.cons_logger.warning(message)
		elif level =='error':
			self.cons_logger.error(message)
		elif level =='critical':
			self.cons_logger.critical(message)


	def log_message(self, message, level='info'):
		"""
		The method outputs the message on log file or console.
		The method always logs on file and output to console if the log destination is set to console
		or the debug is enabled.
		"""
		self.__log_file(message, level)
		if self.args["log_dest"]  == 'console' or self.args["debug"] :
			self.__log_console(message, level)

	def __init_logger(self):
		"""
		The method initialise a new logger object using the configuration parameters.
		The formatter is different if the debug option is enabler or not.
		The method returns a new logger object and sets the logger's file descriptor in the class variable
		logger_fds, used when the process is demonised.

		:return: list with logger and file descriptor
		:rtype: list

		"""
		log_dir = os.path.expanduser(self.args["log_dir"])
		if not os.path.isdir(log_dir):
			print ("creating directory %s" % log_dir)
			os.makedirs(log_dir,  exist_ok=True)
		log_level = self.args["log_level"]
		log_dest = self.args["log_dest"]

		self.log_dest = log_dest
		log_days_keep = int(self.args["log_days_keep"])
		log_name = "repack_%s" % (self.args["config_name"] )
		log_file = '%s/%s.log' % (log_dir,log_name)
		str_format = "%(asctime)s %(processName)s %(levelname)s %(filename)s (%(lineno)s): %(message)s"
		formatter = logging.Formatter(str_format, "%Y-%m-%d %H:%M:%S")

		sh=logging.StreamHandler(sys.stdout)
		fh = TimedRotatingFileHandler(log_file, when="d",interval=1,backupCount=log_days_keep)
		if log_level=='debug' or self.args["debug"]:
			fh.setLevel(logging.DEBUG)
			sh.setLevel(logging.DEBUG)
		elif log_level=='info':
			fh.setLevel(logging.INFO)
			sh.setLevel(logging.INFO)
		elif log_level=='warning':
			fh.setLevel(logging.WARNING)
			sh.setLevel(logging.WARNING)

		self.file_logger = logging.getLogger('file')
		self.cons_logger = logging.getLogger('console')
		self.file_logger.setLevel(logging.DEBUG)
		self.cons_logger.setLevel(logging.DEBUG)
		fh.setFormatter(formatter)
		sh.setFormatter(formatter)

		self.file_logger.addHandler(fh)
		self.cons_logger.addHandler(sh)
		self.file_logger_fds = fh.stream.fileno()
		self.cons_logger_fds = sh.stream.fileno()

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
		self.config_name = config_file [0]
		self.connection = None
		self.__tables_config = {}
		lib_dir = os.path.dirname(os.path.realpath(__file__))
		rep_dir = "%s/.%s" % (os.path.expanduser('~'),  app_dir)


		local_conf = "%s/%s" % ( rep_dir, config_dir )
		self.global_conf_example = '%s/%s/config-example.toml' % (lib_dir, config_dir, )
		self.local_conf_example = '%s/config-example.toml' % local_conf


		local_logs = "%s/logs/" % rep_dir
		local_pid = "%s/pid/" % rep_dir
		self.__table_conf_dir = "%s/table_conf/" % local_conf

		self.global_table_conf_example = '%s/%s/config-example_repack.toml' % (lib_dir, config_dir, )
		self.local_table_conf_example = '%s/config-example_repack.toml' % self.__table_conf_dir

		self.conf_dirs=[
			rep_dir,
			local_conf,
			local_logs,
			local_pid,
			self.__table_conf_dir,
		]
		self.__set_configuration_files()
		self.__set_conf_permissions(rep_dir)

		if config_file [-1]=='toml':
			self.config_file = "%s/%s" % (local_conf,  self.args.config )
		else:
			self.config_file = "%s/%s.toml" % (local_conf,  self.args.config )
		self.__load_config()
		self.__load_table_config()
		self.pg_engine = pg_engine()

		log_args={}
		log_args["log_dir"] = self.config["logging"]["log_dir"]
		log_args["log_dest"] = self.config["logging"]["log_dest"]
		log_args["log_level"] = self.config["logging"]["log_level"]
		log_args["log_days_keep"] = self.config["logging"]["log_days_keep"]
		log_args["config_name"] = self.config_name
		log_args["debug"] = self.args.debug
		self.logger = rep_logger(log_args)
		self.pg_engine.logger = self.logger
		self.notifier = rep_notifier(self.config["notifier"])
		self.notifier.logger=self.logger
		self.replay_pid = os.path.expanduser('%s/replay_%s.pid' % (self.config["pid_dir"],self.args.config))
		self.prepare_pid = os.path.expanduser('%s/prepare_%s.pid' % (self.config["pid_dir"],self.args.config))
		self.repack_pid = os.path.expanduser('%s/repack_%s.pid' % (self.config["pid_dir"],self.args.config))

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
		self.connection = self.config["connections"]
	def __load_table_config(self):
		"""
		Tries to load the per table settings saved in the directory self.__table_conf_dir in the form of <configuration>_<connection>.toml
		The data is stored in the dictionary self.__tables_config
		"""
		config_file = open(self.config_file, 'r')
		self.config = toml.loads(config_file.read())
		config_file.close()

		for connection in self.config["connections"]:
			table_config_file= '%s/%s_%s.toml' % (self.__table_conf_dir, self.config_name, connection )
			if os.path.isfile(table_config_file):
				config_file = open(table_config_file, 'r')
				table_config = toml.loads(config_file.read())
				config_file.close()
				self.__tables_config[connection] = table_config

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

	def __repack_tables(self):
		"""
		The method performs the real repack
		"""
		self.__check_connections()
		self.pg_engine.connections = self.config["connections"]
		self.pg_engine.tables_config=self.__tables_config
		self.pg_engine.repack_tables(self.connection, self.args.connection )
		msg_notify = "The repack tables is complete. \nTables processed:\n%s" % "\n".join(self.pg_engine.tables_repacked)
		self.logger.log_message('The repack process for configuration %s is complete.' % (self.args.config, ), 'info')
		self.notifier.send_notification('Repack tables complete', msg_notify)

	def __replay_data(self):
		"""
		The method performs the replay of the table's data before the swap independently from the swap procedure
		"""
		self.__check_connections()
		self.pg_engine.connections = self.config["connections"]
		self.pg_engine.tables_config=self.__tables_config
		self.pg_engine.replay_data(self.connection, self.args.connection )

	def __terminate_replay(self, signal, frame):
		self.logger.log_message("Caught stop replay signal. Terminating the replay daemon." , 'info')
		try:
			self.replay_daemon.terminate()
		except:
			pass
		sys.exit(0)

	def __terminate_repack(self, signal, frame):
		self.logger.log_message("Caught stop repack signal. Terminating the repack process." , 'info')
		try:
			self.replay_daemon.terminate()
		except:
			pass
		sys.exit(0)


	def __terminate_prepare(self, signal, frame):
		self.logger.log_message("Caught stop prepare signal. Terminating the prepare process." , 'info')
		try:
			self.prepare_daemon.terminate()
		except:
			pass
		sys.exit(0)

	def replay_data(self):
		signal.signal(signal.SIGINT, self.__terminate_replay)
		if self.args.debug:
			self.logger.args["log_dest"]="console"
			self.__replay_data()
		else:
			if self.config["logging"]["log_dest"]  == 'console':
				foreground = True
			else:
				foreground = False
				print("Replay data for the prepared tables started.")
			keep_fds = [self.logger.file_logger_fds , self.logger.cons_logger_fds , ]
			self.logger.log_message('Starting the replay process for configurantion %s.' % (self.args.config, ), 'info')
			self.replay_daemon = Daemonize(app="replay_data", pid=self.replay_pid, action=self.__replay_data, foreground=foreground , keep_fds=keep_fds)
			self.replay_daemon.start()

	def stop_replay(self):
		"""
			The method reads the pid of the replay process then use it to terminate the background process with signal 2.
		"""
		if os.path.isfile(self.replay_pid):
			try:
				file_pid=open(self.replay_pid,'r')
				pid=file_pid.read()
				file_pid.close()
				os.kill(int(pid),2)
				print("Requesting the replay process for configuration %s to stop" % (self.args.config))
				while True:
					try:
						os.kill(int(pid),0)
					except:
						break
				print("The replay process is stopped")
			except:
				print("The replay process for the configuration %s is already stopped" % (self.args.config))

	def stop_repack(self):
		"""
			The method reads the pid of the repack process then use it to terminate the background process with signal 2.
		"""
		if os.path.isfile(self.repack_pid):
			try:
				file_pid=open(self.repack_pid,'r')
				pid=file_pid.read()
				file_pid.close()
				os.kill(int(pid),2)
				print("Requesting the repack process for configuration %s to stop" % (self.args.config))
				while True:
					try:
						os.kill(int(pid),0)
					except:
						break
				print("The repack process is stopped")
			except:
				print("The repack process for the configuration %s is already stopped" % (self.args.config))

	def stop_prepare(self):
		"""
			The method reads the pid of the prepare repack process then use it to terminate the background process with signal 2.
		"""
		if os.path.isfile(self.prepare_pid):
			try:
				file_pid=open(self.prepare_pid,'r')
				pid=file_pid.read()
				file_pid.close()
				os.kill(int(pid),2)
				print("Requesting the prepare repack process for configuration %s to stop" % (self.args.config))
				while True:
					try:
						os.kill(int(pid),0)
					except:
						break
				print("The prepare repack process is stopped")
			except:
				print("The prepare repack process for the configuration %s is already stopped" % (self.args.config))


	def repack_tables(self):
		"""
		The method starts the repack process
		"""
		self.stop_replay()
		signal.signal(signal.SIGINT, self.__terminate_repack)
		if self.args.debug:
			self.logger.args["log_dest"]="console"
			self.__repack_tables()
		else:
			if self.config["logging"]["log_dest"]  == 'console':
				foreground = True
			else:
				foreground = False
				print("Repack tables process started.")
			keep_fds = [self.logger.file_logger_fds , self.logger.cons_logger_fds , ]
			self.logger.log_message('Starting the repack process for configurantion %s.' % (self.args.config, ), 'info')
			repack_daemon = Daemonize(app="repack_tables", pid=self.repack_pid, action=self.__repack_tables, foreground=foreground , keep_fds=keep_fds)
			repack_daemon.start()

	def abort_repack(self):
		"""
		The method drops the prepared repack removing the log tables, the triggers and the copies.
		"""
		self.stop_replay()
		self.stop_repack()
		self.stop_prepare()
		self.__check_connections()
		self.pg_engine.connections = self.config["connections"]
		self.pg_engine.tables_config=self.__tables_config
		self.pg_engine.abort_repack(self.connection, self.args.connection )
		self.logger.log_message('The repack process for configuration %s is complete.' % (self.args.config, ), 'info')




	def prepare_repack(self):
		self.stop_replay()
		signal.signal(signal.SIGINT, self.__terminate_prepare)
		if self.args.debug:
			self.logger.args["log_dest"]="console"
			self.__prepare_repack()
		else:
			if self.config["logging"]["log_dest"]  == 'console':
				foreground = True
			else:
				foreground = False
				print("Prepare repack process started.")
			keep_fds = [self.logger.file_logger_fds , self.logger.cons_logger_fds , ]
			self.logger.log_message('Starting the repack process for configurantion %s.' % (self.args.config, ), 'info')
			prepare_daemon = Daemonize(app="repack_tables", pid=self.prepare_pid, action=self.__prepare_repack, foreground=foreground , keep_fds=keep_fds)
			prepare_daemon.start()

	def __prepare_repack(self):
		"""
		The method performs the repack
		"""
		self.__check_connections()
		self.pg_engine.connections = self.config["connections"]
		self.pg_engine.tables_config=self.__tables_config
		self.pg_engine.prepare_repack(self.connection, self.args.connection )
		msg_notify = "The prepare repack is complete. You can now run the repack_tables command to finalise the swap.\nTables processed:\n%s" % "\n".join(self.pg_engine.tables_repacked)
		self.notifier.send_notification('Prepare repack complete', msg_notify)
		self.logger.log_message('The prepare repack process for configuration %s is complete.' % (self.args.config, ), 'info')
		if self.args.start_replay:
			self.replay_data()

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

		if os.path.isfile(self.local_table_conf_example):
			if os.path.getctime(self.global_table_conf_example)>os.path.getctime(self.local_table_conf_example):
				print ("updating per table configuration example with %s" % self.local_table_conf_example)
				copy(self.global_table_conf_example, self.local_table_conf_example)
		else:
			print ("copying configuration  example in %s" % self.local_table_conf_example)
			copy(self.global_table_conf_example, self.local_table_conf_example)
