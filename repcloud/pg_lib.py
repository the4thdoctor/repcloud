import psycopg2
from psycopg2 import sql
import multiprocessing as mp
import sys
import os
import time
class pg_engine(object):
	def __init__(self):
		"""
		class constructor, set the useful variables
		"""

		lib_dir = os.path.dirname(os.path.realpath(__file__))
		self.sql_dir = "%s/sql/" % lib_dir
		self.connections = None
		self.__tab_list = None
		self.__tables_config = None
		self.__storage_params = None
		self.__id_table = None
		# repack_step 0 to 8, each step may be resumed
		self.__repack_list = [ 'create table','copy', 'create pkey','create indices', 'replay','swap tables','swap aborted','validate','complete' ]
		self.__application_name = "repcloud - Table: %s [%s] "

	def __check_replica_schema(self, db_handler):
		"""
		The method checks if the sch_repcloud exists

		:return: count from pg_namespace
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
		backend_pid = pgsql_conn.get_backend_pid()
		db_handler = {}
		db_handler["connection"] = pgsql_conn
		db_handler["cursor"] = pgsql_cur
		db_handler["pid"] = backend_pid
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
		sql_filter= ""
		tables = self.connections[con]["tables"]
		schemas = self.connections[con]["schemas"]
		filter = []
		db_handler = self.__connect_db(self.connections[con])
		schema_exists = self.__check_replica_schema(db_handler)
		if schema_exists[0]:
			if len(tables)>0:
				filter.append((db_handler["cursor"].mogrify("format('%%I.%%I',v_schema,v_table) = ANY(%s)",  (tables, ))).decode())
			if len(schemas)>0:
				filter.append((db_handler["cursor"].mogrify("v_schema = ANY(%s)",  (schemas, ))).decode())
			if len(filter)>0:
				sql_filter="AND (%s)" % " OR ".join(filter)
			sql_tab = """
				SELECT
					format('%%I.%%I',tab.v_schema,tab.v_table),
					tab.v_schema,
					tab.v_table,
					tab.i_tab_size,
					tab.t_tab_size,
					tab.t_tab_wasted_bytes
				FROM
					sch_repcloud.v_tab_bloat tab
					INNER JOIN pg_constraint con
						ON con.conrelid=tab.o_tab_oid
				WHERE
						con.contype='p'
					AND	v_schema NOT IN ('information_schema','pg_catalog','sch_repcloud','sch_repnew','sch_repdrop')
					AND	v_schema  NOT LIKE 'pg_%%'
				%s
				ORDER BY i_tab_wasted_bytes DESC

			;""" % sql_filter
			db_handler["cursor"].execute(sql_tab)
			self.__tab_list = db_handler["cursor"].fetchall()
			self.__disconnect_db(db_handler)
		else:
			self.logger.args["log_dest"]="console"
			self.logger.log_message("The repack schema is missing. Please run the command create_schema.", 'warning')

	def __check_repack_step(self, db_handler, table):
		"""
			The method retrieves the repack status for the given table.

		"""
		sql_get_status = """
			SELECT
				coalesce(en_repack_step,'complete'),
				i_id_table,
				coalesce(v_status,'complete'),
				b_ready_for_swap
			FROM
				sch_repcloud.t_table_repack
			WHERE
					v_schema_name=%s
				AND v_old_table_name=%s
		;
		"""
		db_handler["cursor"].execute(sql_get_status,  (table[1], table[2], ))
		rep_step = db_handler["cursor"].fetchone()
		if rep_step:
			rep_status = (rep_step[0], rep_step[2],self.__repack_list.index(rep_step[0]), rep_step[3])
			self.__id_table = rep_step[1]
		else:
			rep_status = ('complete', 'complete', self.__repack_list.index('complete'), False )
		return rep_status


	def __update_repack_status(self, db_handler, repack_step, status="in progress"):
		"""
			The method updates the repack status for the processed table
			allowed values range from 0 to 8, each step may be resumed
			the list order is :
				[ "create table","copy", "create pkeys","create index", "replay","swap tables","swap aborted","validate","complete" ]
		"""
		sql_update_step = """
			UPDATE sch_repcloud.t_table_repack
				SET
					en_repack_step=%s,
					v_status=%s
			WHERE
				i_id_table=%s
			RETURNING v_old_table_name
		;
		"""
		sql_app_name = """SET application_name = %s;"""
		db_handler["cursor"].execute(sql_update_step,  (self.__repack_list[repack_step], status, self.__id_table, ))
		tab_rep = db_handler["cursor"].fetchone()
		app_name = self.__application_name % (tab_rep[0], self.__repack_list[repack_step], )
		db_handler["cursor"].execute(sql_app_name,  (app_name, ))

	def __get_table_fillfactor(self, table):
		"""
		Returns the table's fillfactor determined by self.__storage_params or none if there is no storage setting.
		"""
		fillfactor = None
		if self.__storage_params:
			if "fillfactor" in self.__storage_params:
				fillfactor = self.__storage_params["fillfactor"]
				if table[1] in self.__storage_params:
					if "fillfactor" in self.__storage_params[table[1]]:
						fillfactor = self.__storage_params[table[1]]["fillfactor"]
						if table[2] in self.__storage_params[table[1]]:
							if "fillfactor" in self.__storage_params[table[1]][table[2]]:
								fillfactor = self.__storage_params[table[1]][table[2]]["fillfactor"]

		return fillfactor

	def __get_foreign_keys(self, db_handler):
		"""
		Updates the foreign keys in the table
		"""
		sql_clean_fkeys  = """ DELETE FROM  sch_repcloud.t_tab_fkeys  WHERE i_id_table=%s;"""
		sql_get_fkeys = """INSERT INTO sch_repcloud.t_tab_fkeys
				(
					i_id_table,
					t_con_create_new,
					t_con_validate_new,
					t_con_drop_old,
					t_con_create_old,
					t_con_validate_old,
					v_table_name,
					v_schema_name,
					v_con_name
				)
			SELECT
				i_id_table,
				t_con_create_new,
				t_con_validate_new,
				t_con_drop_old,
				t_con_create_old,
				t_con_validate_old,
				v_table_name,
				v_schema_name,
				v_con_name
			FROM sch_repcloud.v_tab_fkeys vtf
			WHERE
				i_id_table=%s;
			"""

		sql_clean_ref_fkeys  = """ DELETE FROM  sch_repcloud.t_tab_ref_fkeys  WHERE i_id_table=%s;"""
		sql_get_ref_fkeys="""
		INSERT INTO sch_repcloud.t_tab_ref_fkeys
		(
			i_id_table,
			t_con_create_new,
			t_con_drop_old,
			t_con_create_old,
			t_con_validate_old,
			t_tab_lock_old,
			v_old_ref_table,
			v_referenced_schema_name,
			v_schema_name,
			v_con_name,
			v_new_ref_table,
			v_referencing_table,
			v_ref_schema_name

		)

		SELECT
			i_id_table,
			t_con_create_new,
			t_con_drop_old,
			t_con_create_old,
			t_con_validate_old,
			t_tab_lock_old,
			v_old_ref_table,
			v_referenced_schema_name,
			v_schema_name,
			v_con_name,
			v_new_ref_table,
			v_referencing_table,
			v_ref_schema_name
		FROM
			sch_repcloud.v_tab_ref_fkeys
		WHERE
				i_id_table=%s;
		;
		"""
		db_handler["cursor"].execute(sql_clean_fkeys,  (self.__id_table, ))
		db_handler["cursor"].execute(sql_get_fkeys,  (self.__id_table, ))
		db_handler["cursor"].execute(sql_clean_ref_fkeys,  (self.__id_table, ))
		db_handler["cursor"].execute(sql_get_ref_fkeys,  (self.__id_table, ))

	def __create_new_table(self, db_handler, table):
		"""
			The method creates a new table in the sch_repcloud schema using the function fn_create_repack_table
		"""
		fillfactor = self.__get_table_fillfactor(table)
		sql_create_new = """SELECT sch_repcloud.fn_create_repack_table(%s,%s,%s); """
		sql_create_log = """SELECT sch_repcloud.fn_create_log_table(%s,%s); """
		self.logger.log_message('Creating a copy of table %s. ' % (table[0],  ), 'info')
		db_handler["cursor"].execute(sql_create_new,  (table[1], table[2], fillfactor, ))
		tab_create = db_handler["cursor"].fetchone()
		self.__id_table = tab_create[0]
		self.logger.log_message('Creating the log table for %s. ' % (table[0],  ), 'info')
		self.__update_repack_status(db_handler, 0, "in progress")
		db_handler["cursor"].execute(sql_create_log,  (table[1], table[2], ))
		self.__update_repack_status(db_handler, 0, "complete")
		self.__get_foreign_keys(db_handler)

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
		self.__update_repack_status(db_handler, 2, "in progress")
		self.logger.log_message('Creating the primary key %s on table %s. ' % (pkey[2],pkey[1],  ), 'info')
		db_handler["cursor"].execute(pkey[0])
		self.__update_repack_status(db_handler, 2, "complete")


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
		ALTER TABLE %s.%s DISABLE TRIGGER z_repcloud_log;
		ALTER TABLE %s.%s DISABLE TRIGGER z_repcloud_truncate;
		""" % (table[1], table[2],table[1], table[2], )

		sql_drop_trg = """
			DROP TRIGGER z_repcloud_log ON  %s.%s ;
			DROP TRIGGER z_repcloud_truncate ON  %s.%s ;
		""" % (table[1], table[2],table[1], table[2],)

		sql_get_drop_table="""
			SELECT
				format('DROP TABLE sch_repnew.%%I;',v_new_table_name)	as t_drop_new_tab,
				format('DROP TABLE sch_repnew.%%I;',v_log_table_name )	as t_drop_log_tab
			FROM
				sch_repcloud.t_table_repack
			WHERE
					v_schema_name=%s
				AND v_old_table_name=%s
			;
		"""

		db_handler["cursor"].execute(sql_set_lock_timeout,  (lock_timeout,))
		while try_disable:
			try:
				db_handler["cursor"].execute(sql_disable_trg)
				try_disable = False
			except psycopg2.Error as e:
					if e.pgcode == '40P01':
						self.logger.log_message('Deadlock detected. I could not disable the triggers on the table %s.%s' % (table[1], table[2] ), 'info')
						self.logger.log_message("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror), 'error')
					elif e.pgcode=='42704':
						self.logger.log_message('Logging triggers on the table %s.%s already dropped' % (table[1], table[2] ), 'warning')
						break
			except:
				raise
		db_handler["cursor"].execute(sql_reset_lock_timeout,  )
		self.logger.log_message('Dropping the repack table in sch_repnew schema' , 'info')
		db_handler["cursor"].execute(sql_get_drop_table,  (table[1], table[2],  ))
		drop_stat = db_handler["cursor"].fetchone()
		db_handler["cursor"].execute(drop_stat[0])
		db_handler["cursor"].execute(sql_set_lock_timeout,  (lock_timeout,))
		while try_drop:
			try:
				db_handler["cursor"].execute(sql_drop_trg)
				db_handler["cursor"].execute(drop_stat[1])
				try_drop = False
			except psycopg2.Error as e:
					self.logger.log_message('An error occurred during the drop of the table %s.%s' % (table[1], table[2] ), 'info')
					self.logger.log_message("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror), 'error')
			except:
				raise
			db_handler["cursor"].execute(sql_reset_lock_timeout,  )

	def __check_consistent_reachable(self, db_handler, table, con):
		"""
			The method runs two queries on the pg_stat_user_tables to determine the
			amount of changes in the time defined by check_time. Then  returns an estimated amount of max_replay_rows
			using that information.
			Then checks if the consistent status can be reached for the given table timing the replay function's run
		"""
		max_replay_rows = self.connections[con]["max_replay_rows"]
		check_time = int(self.connections[con]["check_time"])


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
		self.logger.log_message('Initial value is %s.' % (initial_tuples[0], ), 'debug')
		self.logger.log_message('Sleeping %d seconds.' % (check_time,  ), 'info')
		time.sleep(check_time)
		self.logger.log_message('Checking the final value of modified tuples on %s.%s' % (table[1], table[2], ), 'info')
		db_handler["cursor"].execute(sql_get_mod_tuples,  (table[1], table[2],  ))
		final_tuples = db_handler["cursor"].fetchone()
		update_rate = (int(final_tuples[0])-int(initial_tuples[0]))/60
		self.logger.log_message('The rate of the modified tuples on %s.%s is %d tuples/second' % (table[1], table[2], update_rate, ), 'info')
		self.logger.log_message('The final value is %s.' % (final_tuples[0], ), 'debug')
		self.logger.log_message('Checking the replay speed of %s tuples on %s.%s' % (max_replay_rows, table[1], table[2], ), 'info')
		start_replay = time.time()
		db_handler["cursor"].execute(sql_replay_data,  (table[1], table[2],max_replay_rows,  ))
		end_replay = time.time()
		replay_time = end_replay- start_replay
		replay_rate = int(max_replay_rows)/replay_time
		self.logger.log_message('The procedure replayed on %s.%s %s in %s seconds' % (table[1], table[2], max_replay_rows,replay_time,  ), 'debug')
		self.logger.log_message('The replay rate on %s.%s is %s tuples/second' % (table[1], table[2], replay_rate, ), 'info')


		if replay_rate>update_rate:
			self.logger.log_message('The replay rate on %s.%s is sufficient to reach the consistent status.' % (table[1], table[2],  ), 'info')
			return True
		else:
			self.logger.log_message('The replay rate on %s.%s is not sufficient to reach the consistent status. Aborting the repack.' % (table[1], table[2],  ), 'info')
			return False

	def __replay_data(self, table, con, queue_swap,queue_replay):
		"""
		The method performs a replay on the specified table using the connection parameters.
		The method creates a new database connection for performing its task.
		The mp queue is used to communicate with the invoking method.
		"""
		final_replay = False
		max_replay_rows = self.connections[con]["max_replay_rows"]
		sql_replay_data = """SELECT sch_repcloud.fn_replay_change(%s,%s,%s);"""
		db_handler = self.__connect_db(self.connections[con])

		self.logger.log_message('Starting the replay loop for %s.' %(table[0], ) , 'info')
		while True:
			db_handler["cursor"].execute(sql_replay_data,  (table[1], table[2],max_replay_rows  ))
			remaining_rows = db_handler["cursor"].fetchone()
			if final_replay and remaining_rows[0]==0:
				self.logger.log_message('All rows replayed. Signalling the caller.', 'debug')
				queue_replay.put(True)
				break
			elif remaining_rows[0]==0:
				self.logger.log_message('All queued rows replayed, signalling the caller.', 'debug')
				queue_replay.put(True)
				self.logger.log_message('Waiting for the final replay signal.' , 'debug')
				final_replay = queue_swap.get()
				self.logger.log_message('Starting the final replay.' , 'debug')
			else:
				self.logger.log_message('There are still rows to replay.' , 'debug')


		self.logger.log_message('End of the replay loop for table %s' %(table[0], ) , 'info')
		self.__disconnect_db(db_handler)

	def __watchdog(self, table, con, pid_swap ):
		"""
		The method checks for potential deadlocks and acts according to the strategy set in the parameter deadlock_resolution.
		"""
		deadlock_resolution = self.connections[con]["deadlock_resolution"]
		db_handler = self.__connect_db(self.connections[con])

		self.logger.log_message('Start of the watchdog loop.' , 'info')
		sql_cancel = """
		SELECT
			blocking_pid,
			cancel_statement,
			terminate_statement
		FROM
			sch_repcloud.v_blocking_pids vbp
		WHERE
			blocked_pid=%s
		;
		"""
		while True:
			db_handler["cursor"].execute(sql_cancel,  (pid_swap,  ))
			blocking_processes = db_handler["cursor"].fetchall()
			if len(blocking_processes)>0:
				self.logger.log_message('Potential deadlock detected. Process %s cannot proceed with the table swap.' % (pid_swap, ) , 'debug')
				if deadlock_resolution == "nothing":
					self.logger.log_message("Deadlock resolution set to nothing. Waiting for the database's deadlock resolution." , 'warning')
					break
				else:
					if deadlock_resolution== "cancel_query":
						self.logger.log_message('Cancelling the locking queries.' , 'debug')
						stat_id = 1
					elif deadlock_resolution== "kill_query":
						self.logger.log_message('Terminating the locking queries.' , 'debug')
						stat_id = 2
					for blk_proc in blocking_processes:
						try:
							db_handler["cursor"].execute(blk_proc[stat_id])
						except:
							self.logger.log_message("Couldn't cancel or terminate the query with pid %s." % (blk_proc[stat_id], ) , 'error')
		self.__disconnect_db(db_handler)

	def __get_vxid(self, db_handler):
		"""
		Get the virtual transaction id active at the moment of the query
		"""
		sql_get_virtual_xid = """SELECT x_vxids FROM sch_repcloud.v_virtual_txd ; """
		db_handler["cursor"].execute(sql_get_virtual_xid)
		vxid = db_handler["cursor"].fetchone()[0]
		return vxid

	def __wait_for_vxid(self, db_handler, vxid):
		"""
		Waits for the virtual transaction id active at the moment of the query to commit
		"""
		sql_get_xid_alive = """SELECT pid FROM pg_locks WHERE locktype = 'virtualxid' AND pid <> pg_backend_pid() AND virtualtransaction = ANY(%s)"""
		while True:
			db_handler["cursor"].execute(sql_get_xid_alive, (vxid, ))
			xid_alive = db_handler["cursor"].fetchall()
			self.logger.log_message('Waiting for %s transactions to finish' % (len(xid_alive),  ), 'info')
			time.sleep(1)
			if len(xid_alive)==0:
				break

	def __swap_tables(self, db_handler, table, con):
		"""
		The method replays the table's data then tries to swap the origin table with the new one
		"""
		lock_timeout = self.connections[con]["lock_timeout"]

		#define the SQL statements required for the swap
		sql_set_lock_timeout = """SET lock_timeout = %s;"""
		sql_reset_lock_timeout = """SET lock_timeout = default;"""
		sql_lock_table = """LOCK TABLE "%s"."%s" IN ACCESS EXCLUSIVE MODE; """ % (table[1], table[2],)


		sql_seq = """
			SELECT
				format(
					'SELECT setval(''%%I.%%I''::regclass,%%L);',
					nspname,
					refname,
					(last_value+(increment_by*cache_size))
				)


			FROM
			(
				SELECT
					ser.secatt,
					ser.refname,
					ser.nspname,
					ser.refobjid::regclass::TEXT AS seqtab,
					seq.increment_by,
					seq.cache_size,
					seq.last_value,
					nt.i_id_table

				FROM
					sch_repcloud.v_serials ser
					INNER JOIN sch_repcloud.t_table_repack nt
					ON ser.refobjid=nt.oid_new_table
					INNER JOIN pg_sequences seq
					ON
							seq.schemaname=nt.v_schema_name
						AND seq.sequencename=ser.refname

			) setv
			WHERE
				i_id_table=%s
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
						vie.t_drop_view
				END AS t_drop_view,
				CASE
					WHEN vie.i_id_table IS NOT NULL
					THEN
						vie.t_create_view
				END AS t_create_view,
				vie.v_view_name,
				tab.v_schema_name,
				tab.v_old_table_name,
				tab.v_new_table_name,
				tab.v_log_table_name

			FROM
				sch_repcloud.t_table_repack tab
				LEFT OUTER JOIN sch_repcloud.t_view_def vie
					ON vie.i_id_table=tab.i_id_table
			WHERE
						tab.v_schema_name=%s
					AND tab.v_old_table_name=%s
				;
		"""

		sql_drop_view = """
			SELECT
				v_view_name,
				t_drop_view
			FROM
				sch_repcloud.t_view_def
			WHERE
				i_id_table=%s
			ORDER BY
				i_drop_order;
		"""
		sql_create_view = """
			SELECT
				v_view_name,
				t_create_view
			FROM
				sch_repcloud.t_view_def
			WHERE
				i_id_table=%s
			ORDER BY
				i_create_order;
		"""

		sql_drop_trg = """
			DROP TRIGGER z_repcloud_log ON  sch_repdrop.%s ;
			DROP TRIGGER z_repcloud_truncate ON  sch_repdrop.%s ;
		""" % (table[2], table[2],)

		sql_drop_log_table = """DROP TABLE sch_repnew.%s; """

		sql_drop_old_table = """
			DROP TABLE sch_repdrop.%s ;
		""" % (table[2],)

		# determine whether we need to drop and recreate the attached views to the repacked table
		db_handler["cursor"].execute(sql_drop_view,  (self.__id_table, ))
		view_drop = db_handler["cursor"].fetchall()
		if view_drop:
			db_handler["cursor"].execute(sql_create_view,  (self.__id_table, ))
			view_create = db_handler["cursor"].fetchall()

		# build the statements for the table swap
		db_handler["cursor"].execute(sql_swap,  (table[1], table[2], ))
		table_swap = db_handler["cursor"].fetchall()

		self.__update_repack_status(db_handler, 4, "in progress")
		queue_swap = mp.Queue()
		queue_replay = mp.Queue()
		replay_daemon = mp.Process(target=self.__replay_data, name='replay_process', daemon=True, args=(table, con, queue_swap,queue_replay))
		replay_daemon.start()
		while True:
			try:
				replay_daemon.terminate()
			except:
				self.logger.log_message('The replay daemon is not running.' , 'warn')
			queue_swap = None
			queue_swap = mp.Queue()
			queue_replay = None
			queue_replay = mp.Queue()
			replay_daemon = mp.Process(target=self.__replay_data, name='replay_process', daemon=True, args=(table, con, queue_swap,queue_replay))
			replay_daemon.start()
			self.logger.log_message('Waiting for the can swap signal.' , 'info')
			try_swap = queue_replay.get()
			if try_swap:
				self.__update_repack_status(db_handler, 5, "in progress")
				# set the lock_timeout we don't want to keep the other sessions lock in wait more than necessary
				db_handler["cursor"].execute(sql_set_lock_timeout,  (lock_timeout,))
				#set the autocommit off as we want to rollback safely in case of failure
				db_handler["connection"].set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE, autocommit=False)
				try:
					vxid = self.__get_vxid(db_handler)
					self.__wait_for_vxid(db_handler, vxid)
					# try to acquire an exclusive lock on the repacked table
					self.logger.log_message('Trying to acquire an exclusive lock on the table %s.%s' % (table[1], table[2] ), 'info')
					db_handler["cursor"].execute(sql_lock_table)
					self.logger.log_message('Starting the watchdog process', 'info')
					self.logger.log_message('Signalling the replay daemon for the final replay.', 'info')
					queue_swap.put(True)
					self.logger.log_message('Waiting for the final replay to complete.', 'info')
					can_swap = queue_replay.get()
					if can_swap:
						watchdog_daemon = mp.Process(target=self.__watchdog, name='watchdog_process', daemon=True, args=(table, con, db_handler["pid"] ,))
						watchdog_daemon.start()

						#determine which sequences are attached to the original table and reset the same on the new table in order to make them work properly after the swap
						db_handler["cursor"].execute(sql_seq,  (self.__id_table, ))
						reset_sequence = db_handler["cursor"].fetchone()
						if reset_sequence:
							self.logger.log_message("resetting sequence on new table" , 'debug')
							db_handler["cursor"].execute(reset_sequence[0])

						# create the new table's foreign keys
						self.__create_tab_fkeys(db_handler, table)
						self.__create_ref_fkeys(db_handler, table)

						tswap = table_swap[0]
						self.logger.log_message("change schema old table: %s" %tswap[0], 'debug')
						db_handler["cursor"].execute(tswap[0])
						self.logger.log_message("Rename new table: %s" %tswap[1], 'debug')
						db_handler["cursor"].execute(tswap[1])
						self.logger.log_message("change schema new table: %s" %tswap[2], 'debug')
						db_handler["cursor"].execute(tswap[2])

						#drop the views referencing the original table
						if view_drop:
							self.logger.log_message("dropping the views referencing the table ", 'debug')
							for view in view_drop:
								self.logger.log_message("drop view %s" % (view[0]), 'debug')
								db_handler["cursor"].execute(view[1])
							for view in view_create:
								self.logger.log_message("create view %s" % (view[0]), 'debug')
								db_handler["cursor"].execute(view[1])

						#remove the logging triggers from the original table
						self.logger.log_message("Dropping the logging triggers", 'info')
						db_handler["cursor"].execute(sql_drop_trg)

						#remove the log table
						replaced_sql_drop_log_table = sql_drop_log_table % tswap[11]
						self.logger.log_message("Dropping the log table ", 'info')
						db_handler["cursor"].execute(replaced_sql_drop_log_table)

						#remove the old table
						self.logger.log_message("Dropping the old table", 'info')
						db_handler["cursor"].execute(sql_drop_old_table)

						#commit the swap
						db_handler["connection"].commit()

						#terminating the watchdog
						watchdog_daemon.terminate()

						#wait for the the replay daemon to terminate
						while replay_daemon.is_alive():
							self.logger.log_message("Waiting for the replay daemon to complete", 'info')
							time.sleep(1)

						#exit the loop
						break

				#exception handling
				except psycopg2.Error as e:
					#deadlock during the swap
					if e.pgcode == '40P01':
						self.logger.log_message('Deadlock detected during the swap attempt on the table %s.%s, resuming the replay' % (table[1], table[2] ), 'info')
					#lock not available
					elif e.pgcode == '55P03':
						self.logger.log_message('Could not acquire an exclusive lock on the table %s.%s, resuming the replay' % (table[1], table[2] ), 'info')

					#anything else related with psycopg2
					else:
						self.logger.log_message('An error occurred during the swap attempt on the table %s.%s, resuming the replay' % (table[1], table[2] ), 'info')
						self.logger.log_message("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror), 'error')

					#this section is to manage the risk of having the session killed by the deadlock resolution
					if  db_handler["connection"].closed:
						#we mark the repack as failed and then we remove the table's copy and the logging triggers
						self.logger.log_message('The connection is no longer active, trying to reconnect.' , 'info')
						db_handler = self.__connect_db(self.connections[con])
						self.__remove_table_repack(db_handler, table, con)
						self.__update_repack_status(db_handler, 5, "failed")
					else:
						#if the session is still active we can continue to try the swap
						self.logger.log_message('The connection is still active, continuing.' , 'info')
						#we rollback the swap then we reset the session's autocommit and lock timeout
						db_handler["connection"].rollback()
						db_handler["connection"].set_session(isolation_level=None, autocommit=True)
						db_handler["cursor"].execute(sql_reset_lock_timeout )
						#we turn on the continue_replay so we can continue to replay the rows in the next iteration
						self.__update_repack_status(db_handler, 4,  "in progress")
					try_swap  = False
				except:
					#for any other error we raise an exception
					try_swap  = False
					raise
			# at the end of the swap we reset the autocommit status and  the lock timeout
		db_handler["connection"].set_session(isolation_level=None,autocommit=True)
		db_handler["cursor"].execute(sql_reset_lock_timeout )
		self.__update_repack_status(db_handler, 5, "complete")


	def __set_ready_for_swap(self, db_handler, ready_for_swap):
		"""
		The method set the flag b_ready_for_swap to true at the end of prepare or to false at the end of repack.
		"""
		sql_set_flag = """
		UPDATE sch_repcloud.t_table_repack
			SET
				b_ready_for_swap=%s
		WHERE
			i_id_table=%s
		"""
		db_handler["cursor"].execute(sql_set_flag, (ready_for_swap, self.__id_table, ))

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
				AND v_contype IS NULL
			;
		"""
		self.__update_repack_status(db_handler, 3, "in progress")
		self.logger.log_message('Determining whether we need to create new indices on on table %s.%s. ' % (table[1], table[2],  ), 'info')
		db_handler["cursor"].execute(sql_get_idx,  (table[1], table[2], ))
		idx_list = db_handler["cursor"].fetchall()
		for index in idx_list:
			self.logger.log_message('Creating index %s on table %s. ' % (index[2],index[1],  ), 'info')
			db_handler["cursor"].execute(index[0])
		self.__update_repack_status(db_handler, 3, "complete")
		self.__set_ready_for_swap(db_handler, True)

	def __build_select_list(self, db_handler, table):
		"""
			The method builds the select list using the dictionary self.__tables_config to determine
			whether filter a jsonb field or strip null keys from json,jsonb fields.
		"""
		select_list = []
		insert_list = []
		table_filter = None
		sql_get_fields="""
		SELECT
			col_typ_map
		FROM
			sch_repcloud.v_table_column_types
		WHERE
				schema_name=%s
			AND table_name=%s ;"""
		db_handler["cursor"].execute(sql_get_fields, (table[1], table[2]))
		col_typ_map = (db_handler["cursor"].fetchone())[0]
		try:
			table_filter=self.__tables_config[table[1]][table[2]]
			self.logger.log_message('Found filter definition for table %s.%s' % (table[1], table[2],  ), 'debug')
		except:
			self.logger.log_message('There is no filter definition for table %s.%s' % (table[1], table[2],  ), 'debug')

		for column in col_typ_map:
			if table_filter:
				try:
					col_filter = table_filter[column]
					if "cleanup_nulls" in col_filter:
						self.logger.log_message('Found cleanup nulls for field %s table %s.%s' % (column, table[1], table[2],  ), 'debug')
						if col_filter["cleanup_nulls"]:
							if col_typ_map[column] == "json":
								col_append = "json_strip_nulls(%s)" % (column, )
							elif col_typ_map[column] == "jsonb":
								col_append = "jsonb_strip_nulls(%s)" % (column, )
							else:
								self.logger.log_message("Can't apply the cleanup nulls filter. The column %s is not jsonb or json" % (column,  ), 'warning')
						else:
							col_append = column
					elif "remove_keys" in col_filter:
						if col_typ_map[column] == "jsonb":
							col_append = "%s -'%s'" % (column, "' -'".join(col_filter["remove_keys"]))
						else:
							self.logger.log_message('The column %s is not jsonb' % (column,  ), 'warning')
							col_append = column
				except:
					col_append = column
			else:
				col_append = column
			select_list.append(col_append)
			insert_list.append(column)
		return([insert_list, select_list])


	def __copy_table_data(self, db_handler, table):
		"""
			The method copy the data from the origin's table to the new one
		"""
		att_lists = self.__build_select_list(db_handler, table)

		self.logger.log_message('Creating the logger triggers on the table %s.%s' % (table[1], table[2],  ), 'info')
		self.__update_repack_status(db_handler, 1, "in progress")
		sql_create_data_trigger = """

		CREATE TRIGGER z_repcloud_log
			AFTER INSERT OR UPDATE OR DELETE
			ON %s.%s
			FOR EACH ROW
			EXECUTE PROCEDURE sch_repcloud.fn_log_data()
			;
		"""  % (table[1], table[2], )

		sql_create_truncate_trigger = """
			CREATE TRIGGER z_repcloud_truncate
			AFTER TRUNCATE ON %s.%s
			FOR EACH STATEMENT
			EXECUTE PROCEDURE sch_repcloud.fn_log_truncate()
			;
		"""  % (table[1], table[2], )


		db_handler["cursor"].execute(sql_create_data_trigger )
		db_handler["cursor"].execute(sql_create_truncate_trigger )

		sql_get_new_tab = """
			UPDATE sch_repcloud.t_table_repack
			SET xid_copy_start=split_part(txid_current_snapshot()::text,':',1)::bigint
			WHERE

					v_schema_name=%s
				AND v_old_table_name=%s
			RETURNING v_new_table_name
			;
		"""
		db_handler["connection"].set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE, autocommit=False)
		vxid = self.__get_vxid(db_handler)
		self.__wait_for_vxid(db_handler, vxid)
		db_handler["cursor"].execute(sql_get_new_tab,  (table[1], table[2], ))
		new_table = db_handler["cursor"].fetchone()

		sql_copy = """INSERT INTO sch_repnew.{} (%s) SELECT %s FROM {}.{} ;"""  % (','.join(att_lists[0]), ','.join(att_lists[1]),)
		self.logger.log_message('Copying the data from %s.%s to %s ' % (table[1], table[0],  new_table[0]), 'info')

		sql_copy = sql.SQL(sql_copy).format(sql.Identifier(new_table[0]),sql.Identifier(table[1]), sql.Identifier(table[2]))
		sql_analyze = sql.SQL("ANALYZE sch_repnew.{};").format(sql.Identifier(new_table[0]))

		db_handler["cursor"].execute(sql_copy)
		db_handler["connection"].commit()
		db_handler["connection"].set_session(isolation_level=None,autocommit=True)
		db_handler["cursor"].execute(sql_analyze)
		self.__update_repack_status(db_handler, 1, "complete")

	def __refresh_matviews(self, db_handler, id_tables):
		"""
			The method refreshes all the materialised views attached to the main table and
			creates on them the required indices.
		"""
		sql_get_refresh = """
			SELECT
				vie.i_id_view,
				vie.t_refresh_matview,
				vie.v_view_name
			FROM
				sch_repcloud.t_view_def vie
				INNER JOIN sch_repcloud.t_table_repack tab
				ON tab.i_id_table=vie.i_id_table
			WHERE
						vie.t_refresh_matview IS NOT NULL
					AND tab.i_id_table=ANY(%s)
			ORDER BY
				i_create_order;
		"""

		db_handler["cursor"].execute(sql_get_refresh,  (id_tables, ))
		refresh_view = db_handler["cursor"].fetchall()

		sql_get_idx = """
			SELECT
				unnest(coalesce(vie.t_idx_matview)) AS t_create_idx
			FROM
				sch_repcloud.t_view_def vie
			WHERE
				vie.i_id_view=%s
			;
		"""
		for matview in refresh_view:
			self.logger.log_message('Creating the indices on the materialised view %s if any. ' % (matview[2],  ), 'info')
			db_handler["cursor"].execute(sql_get_idx,  (matview[0], ))
			idx_list = db_handler["cursor"].fetchall()
			for idx in idx_list:
				db_handler["cursor"].execute(idx[0])
			self.logger.log_message('Refreshing the materialised view %s. ' % (matview[2],  ), 'info')
			try:
				db_handler["cursor"].execute(matview[1])
			except psycopg2.Error as e:
				self.logger.log_message('An error occurred during the refresh of the materialised view %s.' %  (matview[2],  ), 'info')
				self.logger.log_message("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror), 'error')

	def __create_tab_fkeys(self, db_handler, table):
		"""
		The method builds the foreign keys from the new table to the existing tables
		"""
		sql_get_fkeys = """
			SELECT
				t_con_create_new,
				t_con_drop_old,
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
			self.logger.log_message('Creating foreign  key %s on the table %s. ' % (fkey[4],fkey[3],  ), 'info')
			db_handler["cursor"].execute(fkey[0])
			self.logger.log_message('Dropping the foreign key %s on the old table %s. ' % (fkey[4],fkey[3],  ), 'info')
			try:
				db_handler["cursor"].execute(fkey[1])
			except psycopg2.Error as e:
				if e.pgcode == '40P01':
					self.logger.log_message('Deadlock detected during the drop of the foreign key %s on the old table %s' % (fkey[4],fkey[3],  ), 'info')
					raise
				else:
					self.logger.log_message('An error occurred during the drop of the foreign key %s on the old table %s' % (fkey[4],fkey[3],  ), 'info')
					self.logger.log_message("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror), 'error')
					raise
			except:
				self.logger.log_message('An generic error occurred during the drop of the foreign key %s on the old table %s' % (fkey[4],fkey[3],  ), 'info')
				raise

	def __validate_fkeys(self, db_handler):
		"""
			The methods tries to validate all  the foreign keys with not valid status
		"""
		self.__update_repack_status(db_handler, 7, "in progress")
		sql_get_validate = """
			SELECT
				t_con_validate,
				v_con_name,
				v_schema_name,
				v_table_name
			FROM
				sch_repcloud.v_fk_validate
			l;		"""
		db_handler["cursor"].execute(sql_get_validate)
		validate_list = db_handler["cursor"].fetchall()
		if len(validate_list)>0:
			for validate_stat in validate_list:
				self.logger.log_message('Validating the foreign key %s on table %s.%s.' % (validate_stat[1], validate_stat[2],validate_stat[3], ),   'info')
				try:
					db_handler["cursor"].execute(validate_stat[0])
				except psycopg2.Error as e:
					self.logger.log_message("Couldn't validate the foreign key %s on table  %s.%s. %s " % (validate_stat[1],validate_stat[2],validate_stat[3], e,  ), 'warning')
		self.__update_repack_status(db_handler, 7, "complete")

	def __create_ref_fkeys(self, db_handler, table):
		"""
		The method builds the referencing foreign keys from the existing  to the new table
		"""
		sql_get_fkeys = """
			SELECT
				t_con_drop_old,
				t_con_create_new,
				t_con_validate_old,
				v_old_ref_table,
				v_schema_name,
				v_con_name,
				v_new_ref_table,
				v_referencing_table
			FROM
				sch_repcloud.t_tab_ref_fkeys
			WHERE
					i_id_table=%s
			;
		"""
		db_handler["cursor"].execute(sql_get_fkeys,  (self.__id_table, ))
		fk_list = db_handler["cursor"].fetchall()
		for fkey in fk_list:
			self.logger.log_message("drop: %s" %fkey[0], 'debug')
			self.logger.log_message("create: %s" %fkey[1], 'debug')
			try:
				self.logger.log_message('Dropping the old  referencing foreign key %s on table %s. ' % (fkey[5],fkey[7],  ), 'info')
				db_handler["cursor"].execute(fkey[0])
				self.logger.log_message('Creating foreign key %s on table %s. ' % (fkey[5],fkey[7],  ), 'info')
				db_handler["cursor"].execute(fkey[1])
			except psycopg2.Error as e:
				if e.pgcode == '40P01':
					self.logger.log_message('Deadlock detected during the drop of the foreign key %s on the referencing table %s' % (fkey[4],fkey[7],  ), 'info')
					raise
				else:
					self.logger.log_message('An error occurred during the drop of the foreign key %s on the referencing table %s' % (fkey[4],fkey[7],  ), 'info')
					self.logger.log_message("SQLCODE: %s SQLERROR: %s" % (e.pgcode, e.pgerror), 'error')
					raise
			except:
				self.logger.log_message('An generic error occurred during the drop of the foreign key %s on the referencing table %s' % (fkey[4],fkey[7],  ), 'info')
				raise


	def __prepare_repack(self, con):
		"""
			The method executes the preparation for repack for each table in self.tab_list
			self.__repack_list = [ 'create table','copy', 'create pkey','create indices', 'replay','swap tables','swap aborted','validate','complete' ]
		"""
		db_handler = self.__connect_db(self.connections[con])
		tables_repacked=[]
		for table in self.__tab_list:
			rep_status = self.__check_repack_step(db_handler, table)
			if rep_status[1] == "complete":
				if rep_status[2] == 8:
					self.logger.log_message('Running prepare repack on  %s. Expected space gain: %s' % (table[0], table[5] ), 'info')
					self.__create_new_table(db_handler, table)
					rep_status = self.__check_repack_step(db_handler, table)
				if rep_status[2] == 0:
					self.__copy_table_data(db_handler, table)
					rep_status = self.__check_repack_step(db_handler, table)
				if rep_status[2] < 2:
					self.__create_pkey(db_handler, table)
					rep_status = self.__check_repack_step(db_handler, table)
				if  rep_status[2] < 3:
					self.__create_indices(db_handler, table)
			else:
				self.logger.log_message("The repack step for the table %s is %s and the status is %s. Skipping the repack." % (table[0], rep_status[0], rep_status[1],  ), 'info')
			tab_status = self.__check_repack_step(db_handler, table)
			tables_repacked.append("Table: %s - Step: %s - Status: %s" %(table[0], tab_status[0], tab_status[1]))
		self.__disconnect_db(db_handler)
		self.tables_repacked = tables_repacked

	def __analyze_tables(self, con):
		"""
			Runs analyze after the repack tables
		"""
		if "analyze_tables" in self.connections[con]:
			analyze_tables = self.connections[con]["analyze_tables"]
		else:
			analyze_tables = True

		if analyze_tables:
			db_handler = self.__connect_db(self.connections[con])
			for table in self.__tab_list:
				sql_analyze = sql.SQL('ANALYZE {}.{};').format(sql.Identifier(table[1]), sql.Identifier(table[2]))
				self.logger.log_message("Running ANALYZE on table %s" % (table[0],  ), 'info')
				db_handler["cursor"].execute(sql_analyze)

			self.__disconnect_db(db_handler)

	def __abort_repack(self, con):
		"""
		The method removes the log table, the triggers and the copy of the tables prepared  for repack with prepare_repack.
		The method takes action also If the table is left in inconsistent status.
		"""
		db_handler = self.__connect_db(self.connections[con])
		for table in self.__tab_list:
			rep_status = self.__check_repack_step(db_handler, table)
			if rep_status[3] == True:
				self.logger.log_message('Aborting the repack for the table %s.' % (table[0], ), 'info')
				try:
					self.__remove_table_repack(db_handler, table, con)
				except:
					self.logger.log_message('The repack for the table %s is already aborted.' % (table[0], ), 'info')
				self.__set_ready_for_swap(db_handler, False)
				self.__update_repack_status(db_handler, 8, "complete")
			elif rep_status[2] == 8:
					self.logger.log_message('The table %s is not prepared for repack. Abort not required.' % (table[0], ), 'info')
			elif rep_status[3] == False and rep_status[1] in ["in progress", "failed"]:
				try:
					self.logger.log_message('Resetting the status for the table %s.' % (table[0], ), 'info')
					self.__remove_table_repack(db_handler, table, con)
				except:
					self.logger.log_message("Couldn't remove the copy and trigger for table %s." % (table[0], ), 'info')
				self.__set_ready_for_swap(db_handler, False)
				self.__update_repack_status(db_handler, 8, "complete")
			else:
				self.logger.log_message('The table %s status is .' % (table[0], ), 'info')



	def __repack_tables(self, con):
		"""
			The method executes the repack operation for each table in self.tab_list
			self.__repack_list = [ 'create table','copy', 'create pkey','create indices', 'replay','swap tables','swap aborted','validate','complete' ]
		"""
		tables_repacked = []
		id_tables = []

		db_handler = self.__connect_db(self.connections[con])

		for table in self.__tab_list:
			rep_status = self.__check_repack_step(db_handler, table)
			if rep_status[1] == "complete":
				if rep_status[2] == 8:
					self.logger.log_message('Running repack on  %s. Expected space gain: %s' % (table[0], table[5] ), 'info')
					self.__create_new_table(db_handler, table)
					rep_status = self.__check_repack_step(db_handler, table)
				if rep_status[2] == 0:
					self.__copy_table_data(db_handler, table)
					rep_status = self.__check_repack_step(db_handler, table)
				if rep_status[2] < 2:
					self.__create_pkey(db_handler, table)
					rep_status = self.__check_repack_step(db_handler, table)
				if  rep_status[2] < 3:
					self.__create_indices(db_handler, table)
					rep_status = self.__check_repack_step(db_handler, table)
				if rep_status[2] < 5 and rep_status[3] == True:
					self.__swap_tables(db_handler, table, con)
					rep_status = self.__check_repack_step(db_handler, table)
				self.__update_repack_status(db_handler, 8, "complete")
				self.__set_ready_for_swap(db_handler, False)
				id_tables.append(self.__id_table)
			else:
				self.logger.log_message("The repack step for the table %s is %s and the status is %s. Skipping the repack." % (table[0], rep_status[0], rep_status[1],  ), 'info')
			tab_status = self.__check_repack_step(db_handler, table)
			tables_repacked.append("Table: %s - Step: %s - Status: %s" %(table[0], tab_status[0], tab_status[1]))
		self.__refresh_matviews(db_handler, id_tables)
		self.__validate_fkeys(db_handler)
		self.__disconnect_db(db_handler)
		self.tables_repacked = tables_repacked

	def __repack_loop(self, con, action='repack'):
		"""
		The method loops trough the tables available for the connection
		"""
		if con in self.tables_config:
			self.__tables_config = self.tables_config[con]
			if "storage" in self.__tables_config:
				self.__storage_params = self.__tables_config["storage"]
			else:
				self.__storage_params = None

		else:
			self.__tables_config = None
			self.__storage_params = None

		self.__get_repack_tables(con)
		if action == 'repack':
			self.__repack_tables(con)
			self.__analyze_tables(con)
		elif action == 'prepare':
			self.__prepare_repack(con)
		elif action == 'abort':
			self.__abort_repack(con)

	def __replay_loop(self, con):
		"""
		The method loops trough the tables and replays the data until the lag is under max_replay_rows. Then moves to the next one.
		"""
		self.__get_repack_tables(con)
		db_handler = self.__connect_db(self.connections[con])
		for table in self.__tab_list:
			rep_status = self.__check_repack_step(db_handler, table)
			if rep_status[1] == "complete" and rep_status[2] < 5:
				self.logger.log_message("Replaying data for table %s." % (table[0], ), 'info')
				queue_swap = mp.Queue()
				queue_replay = mp.Queue()
				replay_daemon = mp.Process(target=self.__replay_data, name='replay_process', daemon=True, args=(table, con, queue_swap,queue_replay))
				replay_daemon.start()
				self.logger.log_message('Waiting for the can swap signal.' , 'info')
				replay_done = queue_replay.get()
				if replay_done :
					queue_swap.put(True)
					while  replay_daemon.is_alive():
						self.logger.log_message("Waiting for the replay daemon to finish." , 'info')
						time.sleep(1)
		self.__disconnect_db(db_handler)


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
			self.logger.log_message('Repacking the tables for the available  connections'  'info')
			for con in connection:
				self.logger.log_message('Repacking the tables for connection %s' % con, 'info')
				self.__repack_loop(con)
		else:
			self.logger.log_message('Repacking the tables for connection %s' % coname, 'info')
			self.__repack_loop(coname)


	def prepare_repack(self, connection, coname):
		if coname == 'all':
			self.logger.log_message('Preparing the repack for all the tables in the available connections'  'info')
			for con in connection:
				self.logger.log_message('Preparing the repack for all the tables defined in the connection %s' % con, 'info')
				self.__repack_loop(con, 'prepare')
		else:
			self.logger.log_message('Preparing the repack for all the tables in connection %s' % coname, 'info')
			self.__repack_loop(coname, 'prepare')

	def replay_data(self, connection, coname):
		"""
		The method loops through the tables already prepared for repack in order to clear the replay lag.
		"""
		while True:
			if coname == 'all':
				self.logger.log_message('Replaying the data on all the tables in the available connections'  'info')
				for con in connection:
					self.logger.log_message('PReplaying the data on all the tables defined in the connection %s' % con, 'info')
					self.__replay_loop(con)
			else:
				self.logger.log_message('Replaying the data on all the tables in connection %s' % coname, 'info')
				self.__replay_loop(coname)
			time.sleep(1)

	def abort_repack(self, connection, coname):
		if coname == 'all':
			self.logger.log_message('Aborting repack for all the tables prepared for repack defined in the available connections'  'info')
			for con in connection:
				self.logger.log_message('Aborting repack for all the tables prepared for repack defined in the connection %s' % con, 'info')
				self.__repack_loop(con, 'abort')
		else:
			self.logger.log_message('Aborting repack for all the tables prepared for repack defined in connection %s' % coname, 'info')
			self.__repack_loop(coname, 'abort')
