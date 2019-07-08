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
		
	
	def __connect_db(self, connection):
		"""
			Connects to PostgreSQL using the parameters stored in connection. T
			he dictionary is built using the parameters set via adding the key dbname to the self.pg_conn dictionary.
			This method's connection and cursors are widely used in the procedure except for the replay process which uses a 
			dedicated connection and cursor.
		"""
		strconn = "dbname=%(database)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s sslmode=%(sslmode)s"  % connection
		pgsql_conn = psycopg2.connect(strconn)
		pgsql_conn.set_session(autocommit=True)
		pgsql_cur = pgsql_conn .cursor()
		db_handler = {}
		db_handler["connection"] = pgsql_conn
		db_handler["cursor"] = pgsql_cur
		return db_handler
		

	def __disconnect_db(self, db_handler):
		"""
			The method disconnects the postgres connection for the db_handler 
		"""
		db_handler["cursor"].close()
		db_handler["connection"].close()
		
	
	def __create_repack_schema(self, connection):
		"""
		"""
		db_handler=self.__connect_db(connection)
		self.__disconnect_db(db_handler)
	
	def setup_schema(self, connection, coname):
		if coname == 'all':
			print('Creating the repack schema on all the connections')
		else:
			print('Creating the repack schema on %s' % coname)
			self.__create_repack_schema(connection[coname])
	
