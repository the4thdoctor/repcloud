import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from distutils.sysconfig import get_python_lib

class pg_engine(object):
	def __init__(self):
		"""
		class constructor, set the useful variables
		"""
		python_lib=get_python_lib()
		self.sql_dir = "%s/repcloud/sql/" % python_lib
		
	def __check_replica_schema(self, db_handler):
		"""
		The method checks if the sch_chameleon exists
		
		:return: count from information_schema.schemata
		:rtype: integer
		"""
		sql_check="""
			SELECT 
				count(*)
			FROM 
				pg_namespace
			WHERE 
				nspname='sch_repcloud'
		"""
			
		db_handler["cursor"].execute(sql_check)
		num_schema = db_handler["cursor"].fetchone()
		return num_schema
	
	def __connect_db(self, connection):
		"""
			Connects to PostgreSQL using the parameters stored in connection. T
			he dictionary is built using the parameters set via adding the key dbname to the self.pg_conn dictionary.
			This method's connection and cursors are widely used in the procedure except for the replay process which uses a 
			dedicated connection and cursor.
			:return: count from information_schema.schemata
			:rtype: integer
		"""
		strconn = "dbname=%(database)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s sslmode=%(sslmode)s"  % connection
		pgsql_conn = psycopg2.connect(strconn)
		pgsql_conn.set_session(autocommit=True)
		pgsql_cur = pgsql_conn .cursor()
		pgsql_cur.execute('SELECT pg_backend_pid();')
		backend_pid = pgsql_cur.fetchone()
		db_handler = {}
		db_handler["connection"] = pgsql_conn
		db_handler["cursor"] = pgsql_cur
		db_handler["pid"] = backend_pid[0]
		return db_handler
		

	def __disconnect_db(self, db_handler):
		"""
			The method disconnects the postgres connection for the db_handler 
		"""
		db_handler["cursor"].close()
		db_handler["connection"].close()
		
	
	def __create_repack_schema(self, connection):
		"""
		The method creates the repack schema for each conection.
		The create schema script is executed from the self.sql_dir location
		"""
		db_handler=self.__connect_db(connection)
		schema_exists = self.__check_replica_schema(db_handler)
		if schema_exists[0]:
			print ("The repack schema is already created")
		else:
			file_schema = open(self.sql_dir+"create_schema.sql", 'r')
			sql_schema = file_schema.read()
			file_schema.close()
			db_handler["cursor"].execute(sql_schema)
		self.__disconnect_db(db_handler)
	
	def create_repack_schema(self, connection, coname):
		"""
			The method runs the __create_repack_schema method for the given connection or 
			for all the available connections
		"""
		if coname == 'all':
			for con in connection:
				print('Creating the repack schema on %s' % con)
				self.__create_repack_schema(connection[con])
		else:
			
			self.__create_repack_schema(connection[coname])
	
	def __drop_repack_schema(self, connection):
		"""
		The method drops the repack schema for each conection.
		The dropschema script is executed from the self.sql_dir location
		"""
		db_handler=self.__connect_db(connection)
		schema_exists = self.__check_replica_schema(db_handler)
		if not schema_exists[0]:
			print ("The repack schema is does not exists")
		else:
			file_schema = open(self.sql_dir+"drop_schema.sql", 'r')
			sql_schema = file_schema.read()
			file_schema.close()
			db_handler["cursor"].execute(sql_schema)
		self.__disconnect_db(db_handler)
	

	def drop_repack_schema(self, connection, coname):
		"""
			The method runs the __create_repack_schema method for the given connection or 
			for all the available connections
		"""
		if coname == 'all':
			for con in connection:
				print('Dropping the repack schema on %s' % con)
				self.__drop_repack_schema(connection[con])
		else:
			
			self.__drop_repack_schema(connection[coname])
	
