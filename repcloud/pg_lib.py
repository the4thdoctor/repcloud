import psycopg2
from psycopg2 import sql
#from psycopg2.extras import RealDictCursor
from distutils.sysconfig import get_python_lib
import sys
import time
class pg_engine(object):
	def __init__(self):
		"""
		class constructor, set the useful variables
		"""
		python_lib=get_python_lib()
		self.sql_dir = "%s/repcloud/sql/" % python_lib
		self.connections = None
		self.__tab_list = None
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
			self.logger.args["log_dest"]="console"
			self.logger.log_message("The repack schema is already created", 'warning')
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
				self.logger.args["log_dest"]="console"
				self.logger.log_message('Creating the repack schema on %s' % con, 'info')
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
			self.logger.args["log_dest"]="console"
			self.logger.log_message("The repack schema is does not exists", 'warning')
			print ("The repack schema is does not exists")
		else:
			file_schema = open(self.sql_dir+"drop_schema.sql", 'r')
			sql_schema = file_schema.read()
			file_schema.close()
			db_handler["cursor"].execute(sql_schema)
		self.__disconnect_db(db_handler)
	
	def __get_repack_tables(self, con):
		"""
		The method generates a list of tables to repack using the parameters
		tables and schemas within the connection string to filter them.
		If both values are empty all the tables are returned.
		"""
		tables = self.connections[con]["tables"]
		schemas = self.connections[con]["schemas"]
		filter = []
		db_handler = self.__connect_db(self.connections[con])
		if len(tables)>0:
			filter.append((db_handler["cursor"].mogrify("format('%%I.%%I',v_schema,v_table) = ANY(%s)",  (tables, ))).decode())
		if len(schemas)>0:
			filter.append((db_handler["cursor"].mogrify("v_schema = ANY(%s)",  (schemas, ))).decode())
		if len(filter)>0:
			sql_filter="WHERE %s" % " OR ".join(filter)
			
		sql_tab = """
			SELECT 
				format('%%I.%%I',v_schema,v_table),
				v_schema,
				v_table,
				i_tab_size,
				t_tab_size,
				t_tab_wasted_bytes
			FROM 
				sch_repcloud.v_tab_bloat
			%s
			ORDER BY i_tab_wasted_bytes DESC
		;""" % sql_filter
		db_handler["cursor"].execute(sql_tab)
		self.__tab_list = db_handler["cursor"].fetchall()
		self.__disconnect_db(db_handler)
	
	def __create_new_table(self, db_handler, table):
		"""
			The method creates a new table in the sch_repcloud schema using the function fn_create_repack_table
		"""
		self.logger.log_message('Creating a copy of table %s. ' % (table[0],  ), 'info')
		sql_create="""SELECT sch_repcloud.fn_create_repack_table(%s,%s); """	
		db_handler["cursor"].execute(sql_create,  (table[1], table[2], ))
		self.logger.log_message('Creating the log table for %s. ' % (table[0],  ), 'info')
		
	def __create_pkey(self, db_handler, table):
		"""
		The method builds the primary key on the given table
		"""
		sql_get_pk = """
			SELECT 
				t_create,
				v_new_table_name,
				t_index_name
			FROM 
				sch_repcloud.v_create_idx_cons
			WHERE 
					v_schema_name=%s
				AND v_old_table_name=%s
				AND v_contype='p'
			;
		"""
		db_handler["cursor"].execute(sql_get_pk,  (table[1], table[2], ))
		pkey = db_handler["cursor"].fetchone()
		self.logger.log_message('Creating the primary key %s on table %s. ' % (pkey[1],pkey[2],  ), 'info')
		db_handler["cursor"].execute(pkey[0])

	def __remove_table_repack(self, db_handler, table, con):
		"""
			The method disables the triggers on the origin's table then 
			removes the new table and cleanup the log_replay table
		"""
		try_disable = True
		try_drop = True
		lock_timeout = self.connections[con]["lock_timeout"]
		sql_set_lock_timeout = """SET lock_timeout = %s;""" 
		sql_reset_lock_timeout = """SET lock_timeout = default;"""
		
		sql_disable_trg = """
		ALTER TABLE %s.%s DISABLE TRIGGER z_repcloud_delete;
		ALTER TABLE %s.%s DISABLE TRIGGER z_repcloud_insert;
		ALTER TABLE %s.%s DISABLE TRIGGER z_repcloud_update;
		ALTER TABLE %s.%s DISABLE TRIGGER z_repcloud_truncate;
		""" % (table[1], table[2],table[1], table[2], table[1], table[2], table[1], table[2],   )

		sql_drop_trg = """
			DROP TRIGGER z_repcloud_insert ON  %s.%s ;
			DROP TRIGGER z_repcloud_delete ON  %s.%s ;
			DROP TRIGGER z_repcloud_update ON  %s.%s ;
			DROP TRIGGER z_repcloud_truncate ON  %s.%s ;
		""" % (table[1], table[2],table[1], table[2], table[1], table[2], table[1], table[2],   )
		sql_cleanup_log_table = """
			DELETE FROM sch_repcloud.t_log_replay 
			WHERE oid_old_tab_oid =
				(
					SELECT
						oid_old_table 
					FROM 
						sch_repcloud.t_table_repack
					WHERE 
							v_schema_name=%s
						AND v_old_table_name=%s
				)
			;

		"""
		sql_get_drop_table="""
			SELECT
				format('DROP TABLE sch_repnew.%%I;',v_new_table_name)	
			FROM 
				sch_repcloud.t_table_repack
			WHERE 
					v_schema_name=%s
				AND v_old_table_name=%s
			;
		"""

		sql_vacuum_full_log = """VACUUM FULL  sch_repcloud.t_log_replay ;"""
		db_handler["cursor"].execute(sql_set_lock_timeout,  (lock_timeout,))
		while try_disable:
			try:
				db_handler["cursor"].execute(sql_disable_trg)
				try_disable = False
			except psycopg2.Error as e:
					self.logger.log_message('Could not acquire an exclusive lock on the table %s.%s for disabling the triggers' % (table[1], table[2] ), 'info')
					self.logger.log_message("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror), 'error')
			except:
				raise
		db_handler["cursor"].execute(sql_reset_lock_timeout,  )
		self.logger.log_message('Cleaning the log table from the %s.%s logged rows' % (table[1], table[2] ), 'info')
		db_handler["cursor"].execute(sql_cleanup_log_table,  (table[1], table[2],  ))
		db_handler["cursor"].execute(sql_vacuum_full_log,  )
		self.logger.log_message('Dropping the repack table in sch_repnew schema' , 'info')
		db_handler["cursor"].execute(sql_get_drop_table,  (table[1], table[2],  ))
		drop_stat = db_handler["cursor"].fetchone()
		db_handler["cursor"].execute(drop_stat[0])
		db_handler["cursor"].execute(sql_set_lock_timeout,  (lock_timeout,))
		while try_drop:
			try:
				db_handler["cursor"].execute(sql_drop_trg)
				try_drop = False
			except psycopg2.Error as e:
					self.logger.log_message('Could not acquire an exclusive lock on the table %s.%s for dropping the triggers' % (table[1], table[2] ), 'info')
					self.logger.log_message("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror), 'error')
			except:
				raise
			db_handler["cursor"].execute(sql_reset_lock_timeout,  )
			
	def __check_consistent_reachable(self, db_handler, table, con):
		"""
			The method runs two queries on the pg_stat_user_tables to determine the
			amount of changes in 60 seconds. Then  returns an estimated amount of max_replay_rows
			using that information.
			Then checks if the consistent status can be reached for the given table timing the replay function's run
		"""
		max_replay_rows = self.connections[con]["max_replay_rows"]
		sql_get_mod_tuples = """
			SELECT
				(n_tup_ins+n_tup_upd+n_tup_del) AS n_tot_mod
			FROM
				pg_stat_user_tables
			WHERE
				schemaname=%s
			AND	relname=%s
		;
		"""
		sql_replay_data = """
			SELECT sch_repcloud.fn_replay_change(%s,%s,%s);
		"""
		
		
		self.logger.log_message('Checking the initial value of modified tuples on %s.%s' % (table[1], table[2], ), 'info')
		db_handler["cursor"].execute(sql_get_mod_tuples,  (table[1], table[2],  ))
		initial_tuples = db_handler["cursor"].fetchone()
		self.logger.log_message('Got %s. Sleeping 60 seconds.' % (initial_tuples[0], ), 'info')
		time.sleep(60)
		self.logger.log_message('Checking the final value of modified tuples on %s.%s' % (table[1], table[2], ), 'info')
		db_handler["cursor"].execute(sql_get_mod_tuples,  (table[1], table[2],  ))
		final_tuples = db_handler["cursor"].fetchone()
		update_rate = (int(final_tuples[0])-int(initial_tuples[0]))/60
		self.logger.log_message('The rate of the modified tuples on %s.%s is %d tuples/second' % (table[1], table[2], update_rate, ), 'info')
		self.logger.log_message('Checking the replay speed of %s tuples on %s.%s' % (max_replay_rows, table[1], table[2], ), 'info')
		start_replay = time.time()
		db_handler["cursor"].execute(sql_replay_data,  (table[1], table[2],max_replay_rows,  ))
		end_replay = time.time()
		replay_time = end_replay- start_replay
		replay_rate = int(max_replay_rows)/replay_time
		self.logger.log_message('The replay rate on %s.%s for %s tuples took %s' % (table[1], table[2], max_replay_rows,replay_time,  ), 'info')
		self.logger.log_message('The replay rate on %s.%s is %s tuples/second' % (table[1], table[2], replay_rate, ), 'info')
		
		
		if replay_rate>update_rate:
			self.logger.log_message('The replay rate on %s.%s is sufficient to reach the consistent status.' % (table[1], table[2],  ), 'info')
			return True
		else:
			self.logger.log_message('The replay rate on %s.%s is not sufficient to reach the consistent status. Aborting the repack.' % (table[1], table[2],  ), 'info')
			return False
			
	
	def __swap_tables(self, db_handler, table, con):
		"""
		The method replays the table's data then tries to swap the origin table with the new one
		"""
		continue_replay = True
		max_replay_rows = self.connections[con]["max_replay_rows"]
		lock_timeout = self.connections[con]["lock_timeout"]
		sql_set_lock_timeout = """SET lock_timeout = %s;""" 
		sql_reset_lock_timeout = """SET lock_timeout = default;"""
		sql_lock_table = """LOCK TABLE "%s"."%s" in ACCESS EXCLUSIVE MODE; """ % (table[1], table[2],)
		sql_replay_data = """
			SELECT sch_repcloud.fn_replay_change(%s,%s,%s);
		"""
		sql_update_sync_xid="""
			UPDATE sch_repcloud.t_table_repack tlog 
				SET
					xid_sync_end=txid_current()
			WHERE
					v_schema_name=%s
				AND	v_old_table_name=%s
			;
		"""
		
		sql_seq = """
			SELECT 
				format(
					'SELECT setval(''%%I.%%I''::regclass,(SELECT max(%%I) FROM %%s));',
					nspname,
					refname,
					secatt,
					seqtab
				)
			FROM
			(
				SELECT 
					secatt,
					refname,
					nspname,
					ser.refobjid::regclass::TEXT AS seqtab
				FROM 
					sch_repcloud.v_serials ser
					INNER JOIN sch_repcloud.t_table_repack nt
					ON ser.refobjid=nt.oid_new_table
				WHERE 
						
						nt.v_schema_name=%s
					AND nt.v_old_table_name=%s
			) setv 	
			;

		"""
		sql_swap = """
			SELECT 
				format('ALTER TABLE %%I.%%I SET SCHEMA sch_repdrop;',v_schema_name,v_old_table_name) AS t_change_old_tab_schema,
				format('ALTER TABLE sch_repnew.%%I RENAME TO %%I;',v_new_table_name,v_old_table_name) AS t_rename_new_table,
				format('ALTER TABLE sch_repnew.%%I SET SCHEMA %%I;',v_old_table_name,v_schema_name) AS t_change_new_tab_schema,
				format('DROP TABLE sch_repdrop.%%I CASCADE;',v_old_table_name,v_schema_name) AS t_change_new_tab_schema,
				coalesce(vie.i_id_table=tab.i_id_table,false) b_views,
				CASE	
					WHEN vie.i_id_table IS NOT NULL
					THEN
						vie.t_change_schema
				END AS t_change_view_schema,
				CASE	
					WHEN vie.i_id_table IS NOT NULL
					THEN
						vie.t_create_view
				END AS t_create_view,
				vie.v_view_name,
				tab.v_schema_name,
				tab.v_old_table_name,
				tab.v_new_table_name

				
			FROM 
				sch_repcloud.t_table_repack tab 
				LEFT OUTER JOIN sch_repcloud.t_view_def vie
					ON vie.i_id_table=tab.i_id_table
			WHERE	
						tab.v_schema_name=%s
					AND tab.v_old_table_name=%s
				;
		"""
			
		sql_check_rows = """
			SELECT 
				count(i_action_id)
			FROM	
				sch_repcloud.t_log_replay tlog
				INNER JOIN sch_repcloud.t_table_repack trep
					ON trep.oid_old_table=tlog.oid_old_tab_oid
				WHERE 
						trep.v_schema_name=%s
					AND trep.v_old_table_name=%s
					AND tlog.i_xid_action>trep.xid_copy_start
			;
		"""
		
		
			
			
		
		while continue_replay :
			self.logger.log_message('Replaying the data on table %s.%s max replay rows per run: %s' % (table[1], table[2],max_replay_rows  ), 'info')
			db_handler["cursor"].execute(sql_replay_data,  (table[1], table[2],max_replay_rows,  ))
			last_replay = db_handler["cursor"].fetchone()
			try_swap = int(last_replay[0])<int(max_replay_rows)
			if try_swap:
				db_handler["cursor"].execute(sql_set_lock_timeout,  (lock_timeout,))
				db_handler["connection"].set_session(autocommit=False)
				self.logger.log_message('Trying to acquire an exclusive lock on the table %s.%s' % (table[1], table[2] ), 'info')
				
				try:
					db_handler["cursor"].execute(sql_lock_table)
					self.logger.log_message('Lock  acquired, checking if we still have a reasonable amount of rows to replay.',  'info')
					db_handler["cursor"].execute(sql_check_rows,  (table[1], table[2],  ))
					last_replay = db_handler["cursor"].fetchone()
					can_swap = int(last_replay[0])<int(max_replay_rows)
					if can_swap:
						self.logger.log_message('Found %s rows left for replay. Starting the swap.' % last_replay[0],  'info')
						continue_replay = False
						run_last_replay = True
						while run_last_replay:
							self.logger.log_message('Replaying the last bunch of data data on table %s.%s  max replay rows per run: %s' % (table[1], table[2],max_replay_rows  ), 'info')
							db_handler["cursor"].execute(sql_replay_data,  (table[1], table[2],max_replay_rows,  ))
							last_replay = db_handler["cursor"].fetchone()
							run_last_replay = int(last_replay[0])>0
						db_handler["cursor"].execute(sql_update_sync_xid,  (table[1], table[2],  ))
						db_handler["cursor"].execute(sql_seq,  (table[1], table[2], ))
						reset_sequence = db_handler["cursor"].fetchone()
						self.logger.log_message("resetting sequence on new table" , 'debug')
						db_handler["cursor"].execute(reset_sequence[0])		
						table_swap = db_handler["cursor"].fetchall()
						db_handler["cursor"].execute(sql_swap,  (table[1], table[2], ))
						table_swap = db_handler["cursor"].fetchall()
						tswap = table_swap[0]
						self.logger.log_message("change schema old table: %s" %tswap[0], 'debug')
						db_handler["cursor"].execute(tswap[0])		
						self.logger.log_message("Rename new table: %s" %tswap[1], 'debug')
						db_handler["cursor"].execute(tswap[1])	
						self.logger.log_message("change schema new table: %s" %tswap[2], 'debug')
						db_handler["cursor"].execute(tswap[2])	
						if  tswap[4]:
							self.logger.log_message("table has views: %s" %tswap[4], 'debug')
							for vswap in table_swap:
								self.logger.log_message("change schema old view", 'debug')
								db_handler["cursor"].execute(vswap[5])		
								self.logger.log_message("create view on new table", 'debug')
								db_handler["cursor"].execute(vswap[6])		
						db_handler["connection"].commit()
					else:
						self.logger.log_message('Found %s rows left for replay. Giving up the swap and resuming the replay.' % last_replay[0],  'info')
						continue_replay = True
						db_handler["connection"].commit()
						
				except psycopg2.Error as e:
					self.logger.log_message('Could not acquire an exclusive lock on the table %s.%s, resuming the replay' % (table[1], table[2] ), 'info')
					self.logger.log_message("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror), 'error')
					db_handler["connection"].rollback()
					try_swap  = False
				except:
					try_swap  = False
					raise
				db_handler["connection"].set_session(autocommit=True)
				db_handler["cursor"].execute(sql_reset_lock_timeout )
				
				
		
	def __create_indices(self, db_handler, table):
		"""
		The method builds the new indices on the new table
		"""
		sql_get_idx = """
			SELECT 
				t_create,
				v_new_table_name,
				t_index_name
			FROM 
				sch_repcloud.v_create_idx_cons
			WHERE 
					v_schema_name=%s
				AND v_old_table_name=%s
				AND v_contype<>'p'
			;
		"""
		db_handler["cursor"].execute(sql_get_idx,  (table[1], table[2], ))
		idx_list = db_handler["cursor"].fetchall()
		for index in idx_list:
			self.logger.log_message('Creating index %s on table %s. ' % (index[1],index[2],  ), 'info')
			db_handler["cursor"].execute(index[0])
			
	def __copy_table_data(self, db_handler, table):
		"""
			The method copy the data from the origin's table to the new one
		"""
		self.logger.log_message('Creating the logger trigger on the table %s.%s' % (table[1], table[0],  ), 'info')
		sql_create_insert_trigger = """
			CREATE TRIGGER z_repcloud_insert
			AFTER INSERT ON %s.%s
			FOR EACH ROW
			EXECUTE PROCEDURE sch_repcloud.fn_log_insert()
			;
		"""  % (table[1], table[2], )
		
		sql_create_update_trigger = """
			CREATE TRIGGER z_repcloud_update
			AFTER UPDATE ON %s.%s
			FOR EACH ROW
			EXECUTE PROCEDURE sch_repcloud.fn_log_update()
			;
		"""  % (table[1], table[2], )
		
		sql_create_delete_trigger = """
			CREATE TRIGGER z_repcloud_delete
			AFTER DELETE ON %s.%s
			FOR EACH ROW
			EXECUTE PROCEDURE sch_repcloud.fn_log_delete()
			;
		"""  % (table[1], table[2], )
		
		sql_create_truncate_trigger = """
			CREATE TRIGGER z_repcloud_truncate
			AFTER TRUNCATE ON %s.%s
			FOR EACH STATEMENT
			EXECUTE PROCEDURE sch_repcloud.fn_log_truncate()
			;
		"""  % (table[1], table[2], )

		sql_create_sync_trigger = """
			CREATE TRIGGER z_repcloud_sync
			AFTER 
			INSERT OR 
			UPDATE OR 
			DELETE ON %s.%s
			FOR EACH ROW
			EXECUTE PROCEDURE sch_repcloud.fn_dml_new_tab()
			;
			ALTER TABLE %s.%s DISABLE TRIGGER z_repcloud_sync;
		"""  % (table[1], table[2],table[1], table[2], )

		
		db_handler["cursor"].execute(sql_create_insert_trigger )
		db_handler["cursor"].execute(sql_create_update_trigger )
		db_handler["cursor"].execute(sql_create_delete_trigger )
		db_handler["cursor"].execute(sql_create_truncate_trigger )
		#db_handler["cursor"].execute(sql_create_sync_trigger )
		
		sql_get_new_tab = """
			UPDATE sch_repcloud.t_table_repack 
			SET xid_copy_start=txid_current()
			WHERE 
					
					v_schema_name=%s
				AND v_old_table_name=%s 
			RETURNING v_new_table_name 
			;
		"""
		db_handler["connection"].set_session(autocommit=False)
		db_handler["cursor"].execute(sql_get_new_tab,  (table[1], table[2], ))
		new_table = db_handler["cursor"].fetchone()
		self.logger.log_message('Copying the data from %s.%s to %s ' % (table[1], table[0],  new_table[0]), 'info')
		
		sql_copy = """
			INSERT INTO sch_repnew.\"%s\" SELECT * FROM \"%s\".\"%s\" ;
		""" % (new_table[0], table[1],table[2], )
		
		sql_analyze = """
			ANALYZE sch_repnew.\"%s\";
		""" % (new_table[0],  )
		
		db_handler["cursor"].execute(sql_copy)
		db_handler["connection"].commit()
		db_handler["connection"].set_session(autocommit=True)
		db_handler["cursor"].execute(sql_analyze)
		
		
	
	def __create_tab_fkeys(self, db_handler, table):
		"""
		The method builds the foreign keys from the new table to the existing tables
		"""
		sql_get_fkeys = """
			SELECT 
				t_con_create,
				t_con_validate,
				v_schema_name,
				v_table_name,
				v_con_name
			FROM
				sch_repcloud.v_tab_fkeys 
			WHERE	
					v_schema_name=%s
				AND v_table_name=%s
			;
		"""
		db_handler["cursor"].execute(sql_get_fkeys,  (table[1], table[2], ))
		fk_list = db_handler["cursor"].fetchall()
		for fkey in fk_list:
			self.logger.log_message('Creating foreign  key %s on table %s. ' % (fkey[4],fkey[3],  ), 'info')
			db_handler["cursor"].execute(fkey[0])		
			
	def __create_ref_fkeys(self, db_handler, table):
		"""
		The method builds the referencing foreign keys from the existing  to the new table 
		"""
		sql_get_fkeys = """
			SELECT 
				t_con_rename,
				t_con_create,
				t_con_validate,
				v_old_ref_table,
				v_schema_name,
				v_con_name,
				v_new_ref_table,
				v_referencing_table
			FROM
				sch_repcloud.v_tab_ref_fkeys 
			WHERE	
					v_schema_name=%s
				AND v_old_ref_table=%s
			;
		"""
		db_handler["cursor"].execute(sql_get_fkeys,  (table[1], table[2], ))
		fk_list = db_handler["cursor"].fetchall()
		for fkey in fk_list:
			self.logger.log_message("rename: %s" %fkey[0], 'debug')
			self.logger.log_message("create: %s" %fkey[1], 'debug')
			self.logger.log_message("validate: %s" %fkey[2], 'debug')
			self.logger.log_message('Renaming the existing foreign key %s on table %s. ' % (fkey[5],fkey[7],  ), 'info')
			db_handler["cursor"].execute(fkey[0])		
			self.logger.log_message('Creating foreign key %s on table %s. ' % (fkey[5],fkey[7],  ), 'info')
			db_handler["cursor"].execute(fkey[1])		
			

	def __swap_tables_static(self, db_handler, table):
		"""
			The method swaps the tables
		"""
		sql_swap="""
			SELECT 
				format('ALTER TABLE %%I.%%I SET SCHEMA sch_repdrop;',v_schema_name,v_old_table_name) AS t_change_old_tab_schema,
				format('ALTER TABLE sch_repnew.%%I RENAME TO %%I;',v_new_table_name,v_old_table_name) AS t_rename_new_table,
				format('ALTER TABLE sch_repnew.%%I SET SCHEMA %%I;',v_old_table_name,v_schema_name) AS t_change_new_tab_schema,
				format('DROP TABLE sch_repdrop.%%I CASCADE;',v_old_table_name,v_schema_name) AS t_change_new_tab_schema,
				coalesce(vie.i_id_table=tab.i_id_table,false) b_views,
				CASE	
					WHEN vie.i_id_table IS NOT NULL
					THEN
						vie.t_change_schema
				END AS t_change_view_schema,
				CASE	
					WHEN vie.i_id_table IS NOT NULL
					THEN
						vie.t_create_view
				END AS t_create_view,
				vie.v_view_name,
				tab.v_schema_name,
				tab.v_old_table_name,
				tab.v_new_table_name

				
			FROM 
				sch_repcloud.t_table_repack tab 
				LEFT OUTER JOIN sch_repcloud.t_view_def vie
					ON vie.i_id_table=tab.i_id_table
			WHERE	
						tab.v_schema_name=%s
					AND tab.v_old_table_name=%s
				;
		"""
		db_handler["cursor"].execute(sql_swap,  (table[1], table[2], ))
		table_swap = db_handler["cursor"].fetchall()
		tswap = table_swap[0]
		self.logger.log_message("change schema old table: %s" %tswap[0], 'debug')
		db_handler["cursor"].execute(tswap[0])		
		self.logger.log_message("Rename new table: %s" %tswap[1], 'debug')
		db_handler["cursor"].execute(tswap[1])	
		self.logger.log_message("change schema new table: %s" %tswap[2], 'debug')
		db_handler["cursor"].execute(tswap[2])	
		if  tswap[4]:
			self.logger.log_message("table has views: %s" %tswap[4], 'debug')
			for vswap in table_swap:
				self.logger.log_message("change schema old view", 'debug')
				db_handler["cursor"].execute(vswap[5])		
				self.logger.log_message("create view on new table", 'debug')
				db_handler["cursor"].execute(vswap[6])		
		
		#self.logger.log_message("drop old table: %s" %tswap[3], 'debug')
		#db_handler["cursor"].execute(tswap[3])		
		
		
	def __repack_tables(self, con):
		"""
			The method executes the repack operation for each table in self.tab_list
		"""
		db_handler = self.__connect_db(self.connections[con])
		for table in self.__tab_list:
			self.logger.log_message('Running repack on  %s. Expected space gain: %s' % (table[0], table[5] ), 'info')
			self.__create_new_table(db_handler, table)
			self.__copy_table_data(db_handler, table)
			self.__create_pkey(db_handler, table)
			self.__create_indices(db_handler, table)
			consistent_reachable = self.__check_consistent_reachable(db_handler, table, con)
			if consistent_reachable:
				self.__swap_tables(db_handler, table, con)
			else:
				self.__remove_table_repack(db_handler, table, con)
		self.__disconnect_db(db_handler)
		
	def __repack_loop(self, con):
		"""
		The method loops trough the tables available for the connection
		"""
		self.__get_repack_tables(con)
		self.__repack_tables(con)

	def drop_repack_schema(self, connection, coname):
		"""
			The method runs the __create_repack_schema method for the given connection or 
			for all the available connections
		"""
		if coname == 'all':
			for con in connection:
				self.logger.args["log_dest"]="console"
				self.logger.log_message('Dropping the repack schema on %s' % con, 'info')
				self.__drop_repack_schema(connection[con])
		else:
			
			self.__drop_repack_schema(connection[coname])
	

	def repack_tables(self, connection, coname):
		if coname == 'all':
			for con in connection:
				self.logger.log_message('Repacking the tables for connection %s' % con, 'info')
				self.__repack_loop(con)
