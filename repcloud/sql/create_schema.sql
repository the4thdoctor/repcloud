--CREATE SCHEMA
CREATE SCHEMA IF NOT EXISTS sch_repcloud;
CREATE SCHEMA IF NOT EXISTS sch_repdrop;
CREATE SCHEMA IF NOT EXISTS sch_repnew;

SET search_path=sch_repcloud;

-- types 
CREATE TYPE ty_repack_step
	AS ENUM ('create table','copy', 'create pkey','create indices', 'replay', 'swap tables','swap aborted','validate','complete');


--VIEWS
CREATE OR REPLACE VIEW v_version 
 AS
	SELECT '0.0.1'::TEXT t_version
;

--TYPES

--TABLES/INDICES	

CREATE TABLE t_table_repack
(
	i_id_table	bigserial,
	oid_old_table oid,
	oid_new_table oid,
	v_old_table_name character varying(100) NOT NULL,
	v_new_table_name  character varying(100) NOT NULL,
	v_log_table_name  character varying(100) NOT NULL,
	v_schema_name character varying(100) NOT NULL,
	t_tab_pk text[],
	en_repack_step ty_repack_step,
	v_status  character varying(100) ,
	i_size_start bigint,
	i_size_end bigint,
	xid_copy_start bigint,
	xid_sync_end bigint,
	ts_repack_start	timestamp without time zone,
	ts_repack_end	timestamp without time zone,
	CONSTRAINT pk_t_table_repack PRIMARY KEY (i_id_table)
)
;

CREATE UNIQUE INDEX uidx_t_table_repack_table_schema ON t_table_repack(v_schema_name,v_old_table_name);

CREATE TABLE sch_repcloud.t_idx_repack (
	i_id_index	bigserial,
	i_id_table	bigint NOT NULL,
	v_table_name character varying(100) NOT NULL,
	v_schema_name character varying(100) NOT NULL,
	b_indisunique bool NULL,
	b_idx_constraint bool NULL,
	v_contype char(1) NULL,
	t_index_name text NOT NULL,
	t_index_def text NULL,
	t_constraint_def text NULL,
	CONSTRAINT pk_t_idx_repack PRIMARY KEY (i_id_index)
);

CREATE UNIQUE INDEX uidx_t_idx_repack_table_schema_INDEX ON t_idx_repack(v_schema_name,v_table_name,t_index_name);

CREATE TABLE sch_repcloud.t_view_def (
	i_id_view	bigserial,
	i_id_table	bigint NOT NULL,
	v_view_name CHARACTER VARYING (100),
	t_change_schema text NOT NULL,
	t_create_view text NOT NULL,
	t_drop_view text NOT NULL,
	t_refresh_matview text NULL,
	t_idx_matview text[] NULL,
	i_create_order integer NOT NULL,
	i_drop_order integer NOT NULL,
	CONSTRAINT pk_t_view_def PRIMARY KEY (i_id_view)
);


-- functions


CREATE OR REPLACE FUNCTION sch_repcloud.fn_replay_change(text,text,integer) 
RETURNS bigint as 
$BODY$
DECLARE
	p_t_schema			ALIAS FOR $1;
	p_t_table			ALIAS FOR $2;
	p_i_max_replay		ALIAS FOR $3;
	v_rec_replay 		record; 
	v_i_action_replay	bigint[];
	v_i_actions 		bigint;
	v_t_tab_data		text[];
	v_t_sql_replay		text;
	v_t_sql_act_rep		text;
	v_t_sql_act 		text;
	v_t_sql_delete 		text;
	v_r_replay			record;
BEGIN


	
	
	v_t_tab_data:=(
		SELECT 
			ARRAY[
				v_new_table_name, -- 1
				array_to_string(array_agg(rec_new),','),-- 2,
				array_to_string(array_agg(rec_old),','),-- 3,
				array_to_string(array_agg(att_list),','),-- 4,
				array_to_string(array_agg(att_markers),','),-- 5,
				array_to_string(array_agg(att_upd),','),-- 6,
				array_to_string(array_agg(att_upd) FILTER (WHERE att_list=ANY(t_tab_pk) ),','),-- 7
				array_to_string(array_agg(rec_old) FILTER (WHERE att_list=ANY(t_tab_pk) ),','), -- 8
				array_to_string(array_agg(rec_new) FILTER (WHERE att_list=ANY(t_tab_pk) ),','),-- 9
				v_log_table_name -- 10
			]
		FROM
		(
			SELECT 
				tab.oid_old_table,
				tab.t_tab_pk,
				tab.v_new_table_name,
				tab.v_log_table_name,
				format('(rec_new_data).%I',att.attname,att.attname) rec_new,
				format('(rec_old_data).%I',att.attname) rec_old,
				format('%I',att.attname) att_list,
				'%L' AS att_markers,
				format('%I=%%L',att.attname) att_upd
			FROM 
				sch_repcloud.t_table_repack tab 
				INNER JOIN pg_catalog.pg_attribute att 
					ON att.attrelid=tab.oid_old_table
			WHERE 
					tab.v_schema_name=p_t_schema
				AND	tab.v_old_table_name=p_t_table
				AND att.attnum>0
				AND NOT att.attisdropped
				
		) tab
		GROUP BY
			v_new_table_name,
			v_log_table_name
	);
	

	v_t_sql_act_rep:=format('
		SELECT 
			array_agg(i_action_id)
		FROM
		(
			SELECT 
				i_action_id
			FROM 
				sch_repnew.%I tlog
			INNER JOIN sch_repcloud.t_table_repack trep
				ON trep.oid_old_table=tlog.oid_old_tab_oid
			WHERE 
					trep.v_schema_name=%L
				AND trep.v_old_table_name=%L
				AND tlog.i_xid_action>trep.xid_copy_start
			LIMIT %L
		) aid
	;
	',
	v_t_tab_data[10],
	p_t_schema,
	p_t_table,
	p_i_max_replay
	);
	EXECUTE v_t_sql_act_rep INTO v_i_action_replay;

	v_t_sql_act:=format('
		SELECT 
			count(i_action_id)
		FROM	
			sch_repnew.%I tlog 
			INNER JOIN sch_repcloud.t_table_repack trep
				ON trep.oid_old_table=tlog.oid_old_tab_oid
			WHERE 
					trep.v_schema_name=%L
				AND trep.v_old_table_name=%L
				AND tlog.i_xid_action>trep.xid_copy_start
				AND tlog.i_action_id NOT IN (SELECT unnest(%L::bigint[]))
	;
	',
	v_t_tab_data[10],
	p_t_schema,
	p_t_table,
	v_i_action_replay
	);

	EXECUTE v_t_sql_act INTO v_i_actions;

	

	v_t_sql_replay:=format(
		'
			SELECT 
				i_action_id,
				i_xid_action,
				v_action,
				CASE 
					WHEN v_action=''INSERT''
					THEN
						format(''INSERT INTO sch_repnew.%%I (%%s) VALUES (%%s);'',
						%L,
						%L,
						format(%L,%s)
						)				
					WHEN v_action=''UPDATE''
					THEN
						format(''UPDATE sch_repnew.%%I SET %%s WHERE %%s'',
						%L,
						format(%L,%s),
						format(%L,%s)
						)
					WHEN v_action=''DELETE''
					THEN
						format(''DELETE FROM sch_repnew.%%I WHERE %%s'',
						%L,
						format(%L,%s)
						)
			
				END
				AS rec_act
				
			FROM 
				sch_repnew.%I
			WHERE i_action_id = ANY(%L)
			ORDER BY 
				i_action_id,
				i_xid_action
			
		',
		v_t_tab_data[1],
		v_t_tab_data[4],
		v_t_tab_data[5],
		v_t_tab_data[2],
		v_t_tab_data[1],
		v_t_tab_data[6],
		v_t_tab_data[2],
		v_t_tab_data[7],
		v_t_tab_data[8],
		v_t_tab_data[1],
		v_t_tab_data[7],
		v_t_tab_data[8],
		v_t_tab_data[10],
		v_i_action_replay
	
	);
	RAISE DEBUG '%',v_t_sql_replay;
	FOR v_r_replay IN EXECUTE v_t_sql_replay
	LOOP
		EXECUTE v_r_replay.rec_act;
	END LOOP;
	v_t_sql_delete:=format(
		'DELETE FROM sch_repnew.%I WHERE i_action_id=ANY(%L::bigint[]);',
		v_t_tab_data[10],
		v_i_action_replay
	
	);	
	EXECUTE v_t_sql_delete;

	RAISE DEBUG '%',v_t_sql_act;
	RETURN v_i_actions;
END;
$BODY$
LANGUAGE plpgsql 
;

 



CREATE OR REPLACE FUNCTION sch_repcloud.fn_create_log_table(text,text) 
RETURNS VOID as 
$BODY$
DECLARE
	p_t_schema			ALIAS FOR $1;
	p_t_table			ALIAS FOR $2;
	v_t_sql_create		text;
	v_t_log				text[];
BEGIN
	v_t_log:=(
		SELECT 
			ARRAY[
				v_log_table_name,
				v_schema_name,
				v_old_table_name,
				oid_old_table::text

				]
		FROM 
			sch_repcloud.t_table_repack
		WHERE	
			v_schema_name=p_t_schema
			AND v_old_table_name=p_t_table
	);
	v_t_sql_create:=format('
		CREATE TABLE IF NOT EXISTS sch_repnew.%I
		(
			i_action_id bigserial ,
			i_xid_action bigint NOT NULL,
			oid_old_tab_oid bigint NOT NULL,
			v_action character varying(20) NOT NULL,
			rec_new_data %I.%I ,
			rec_old_data %I.%I ,
			ts_action timestamp with time zone NOT NULL DEFAULT clock_timestamp()
		
		);
		ALTER TABLE sch_repnew.%I ADD CONSTRAINT pk_t_log_replay_%s PRIMARY KEY (i_action_id);
		CREATE INDEX  idx_xid_action_%s ON sch_repnew.%I  USING btree(i_xid_action);	
		CREATE INDEX  idx_oid_old_tab_oid_%s ON sch_repnew.%I  USING btree(oid_old_tab_oid);
	',
	v_t_log[1], --logtable
	v_t_log[2],	--schema type
	v_t_log[3], --table type
	v_t_log[2],	--schema type
	v_t_log[3], --table type
	v_t_log[1], --logtable
	v_t_log[4], --oid_table
	v_t_log[4], --oid_table
	v_t_log[1], --logtable
	v_t_log[4], --oid_table
	v_t_log[1]  --logtable

	);
	EXECUTE v_t_sql_create;
END;
$BODY$
LANGUAGE plpgsql
;

CREATE OR REPLACE FUNCTION sch_repcloud.fn_create_repack_table(text,text,integer) 
RETURNS bigint as 
$BODY$
DECLARE
	p_t_schema			ALIAS FOR $1;
	p_t_table			ALIAS FOR $2;
	p_i_fillfactor		ALIAS FOR $3;
	v_new_table			character varying(64);
	v_log_table	character varying(64);
	v_i_id_table		bigint;
	t_sql_create 		text;
	t_sql_alter 		text;
	v_oid_old_table		oid;
	v_oid_new_table		oid;
	v_r_sequences		record;
	v_t_seq_name		text[];
BEGIN
	v_oid_old_table:=format('%I.%I',p_t_schema,p_t_table)::regclass::oid;
	v_new_table:=format('%I',p_t_table::character varying(30)||'_'||v_oid_old_table::text);
	v_log_table:=format('%I','log_'||v_oid_old_table::text);

	t_sql_create:=format('
		CREATE TABLE IF NOT EXISTS sch_repnew.%s
			(LIKE %I.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS)',
			v_new_table,
			p_t_schema,
			p_t_table
			
		);
	EXECUTE t_sql_create ;
	IF p_i_fillfactor IS NOT NULL
	THEN
		t_sql_alter=format('ALTER TABLE sch_repnew.%I SET ( fillfactor = %s );',
		v_new_table,
		p_i_fillfactor);
		EXECUTE t_sql_alter ;
	END IF;
	
	v_t_seq_name:=(
		SELECT 
			ARRAY[refname,secatt]::text[]
		FROM 
			sch_repcloud.v_serials 
		WHERE 
			refobjid=v_oid_old_table
	 );
	IF v_t_seq_name IS NOT NULL
	THEN
		t_sql_create:=format('
			CREATE SEQUENCE sch_repnew.%s;',
			v_t_seq_name[1]
		);
		t_sql_alter=format('ALTER TABLE sch_repnew.%I ALTER COLUMN %I SET DEFAULT(nextval(''sch_repnew.%I''::regclass));',
		v_new_table,
		v_t_seq_name[2],
		v_t_seq_name[1]
		);
		EXECUTE t_sql_create;
		EXECUTE t_sql_alter ;
		t_sql_alter=format('ALTER SEQUENCE sch_repnew.%I OWNED BY sch_repnew.%I.%I;',
		v_t_seq_name[1],
		v_new_table,
		v_t_seq_name[2]);
		EXECUTE t_sql_alter ;
	END IF;
	v_oid_new_table:=format('sch_repnew.%I',v_new_table)::regclass::oid;
	INSERT INTO sch_repcloud.t_table_repack 
		(
			oid_old_table,
			v_old_table_name,
			oid_new_table,
			v_new_table_name,
			v_schema_name,
			v_log_table_name
		)
		VALUES 
			(
				v_oid_old_table,
				p_t_table,
				v_oid_new_table,
				v_new_table,
				p_t_schema,
				v_log_table
			) 
		ON CONFLICT (v_schema_name,v_old_table_name)
			DO UPDATE 
				SET 
					v_new_table_name=v_new_table,
					oid_old_table=v_oid_old_table,
					oid_new_table=v_oid_new_table,
					v_log_table_name=v_log_table
		RETURNING i_id_table INTO v_i_id_table
	;	

	UPDATE sch_repcloud.t_table_repack
	SET t_tab_pk=keydat.t_pk_att
	FROM 
		(
			SELECT 
				array_agg(attname) AS t_pk_att,
				relname AS t_table_name,
				conrelid AS oid_conrelid
			FROM
			(
				SELECT 
					attname,
					relname,
					conkey_order,
					conrelid
				FROM
				(
					SELECT 
						unnest(conkey) AS conkey,
						generate_subscripts(conkey,1) AS conkey_order,
						conname,
						contype,
						conrelid,
						tab.relname,
						nsp.nspname,
						typ.typname::text
					FROM 
						pg_catalog.pg_constraint con 
						INNER JOIN pg_catalog.pg_namespace nsp
							ON nsp.oid=con.connamespace
						INNER JOIN pg_class tab
							ON tab.oid=con.conrelid
						INNER JOIN pg_type typ
							ON typ.typrelid=tab.oid
						
					WHERE
						con.contype='p'
				
				) keydat
				INNER JOIN pg_catalog.pg_attribute att 
					ON 
						att.attrelid=keydat.conrelid
					AND att.attnum=conkey
					WHERE
							nspname=p_t_schema
						AND relname=p_t_table
				ORDER BY conkey_order
			) keyagg
			GROUP BY 
				relname,
				conrelid
		) keydat
	WHERE oid_old_table=keydat.oid_conrelid
		;

	DELETE FROM sch_repcloud.t_view_def
	WHERE i_id_table=v_i_id_table;
	
	
	WITH RECURSIVE tabv AS 
	(
		SELECT 
			tab.i_id_table,
			vie.v_view_name,
			vie.t_change_schema,
			vie.t_create_view,
			vie.t_drop_view,
			vie.t_refresh_matview,
			vie.oid_referencing,
			vie.oid_view,
			vie.v_schema_name,
			0 AS create_order,
			0 AS drop_order
		FROM 
			sch_repcloud.v_get_dep_views vie 
			INNER JOIN sch_repcloud.t_table_repack tab 
				ON tab.oid_old_table=vie.oid_referencing
		WHERE 
				tab.i_id_table=v_i_id_table
		 UNION ALL
		 SELECT 
			tab.i_id_table,
			vie.v_view_name,
			vie.t_change_schema,
			vie.t_create_view,
			vie.t_drop_view,
			vie.t_refresh_matview,
			vie.oid_referencing,
			vie.oid_view,
			vie.v_schema_name,
			create_order + 1 AS create_order,
			drop_order - 1 AS drop_order
		FROM 
			sch_repcloud.v_get_dep_views vie 
			INNER JOIN tabv tab
				ON tab.oid_view=vie.oid_referencing
		WHERE
			vie.oid_referencing<>vie.oid_view
		
	)	
	
	INSERT INTO sch_repcloud.t_view_def
			(
				i_id_table,
				v_view_name,
				t_change_schema,
				t_create_view,
				t_drop_view,
				t_refresh_matview,
				t_idx_matview,
				i_create_order,
				i_drop_order
			)
	SELECT 
		i_id_table,
		v_view_name,
		t_change_schema,
		t_create_view,
		t_drop_view,
		t_refresh_matview,
		(
			SELECT 
				array_agg(format('%s;',indexdef)) 
			FROM 
				pg_indexes 
			WHERE 
					tablename = v_view_name
				AND	schemaname = v_schema_name
		),
		create_order,
		drop_order
	FROM tabv
	
	;

		
	DELETE FROM sch_repcloud.t_idx_repack
	WHERE 
			i_id_table=v_i_id_table
	;

	INSERT INTO sch_repcloud.t_idx_repack
			(
				i_id_table,
				v_table_name,
				v_schema_name,
				b_indisunique,
				b_idx_constraint,
				v_contype,
				t_index_name,
				t_index_def,
				t_constraint_def
			)

		SELECT 
			tab.i_id_table,
			tab.v_new_table_name,
			tab.v_schema_name,
			idx.b_indisunique,
			idx.b_idx_constraint,
			idx.v_contype,
			idx.t_index_name,
			idx.t_index_def,
			t_constraint_def
		FROM 
			sch_repcloud.v_token_idx idx 
			INNER JOIN	sch_repcloud.t_table_repack tab
				ON 
						tab.v_old_table_name=idx.v_table
					AND	tab.v_schema_name=idx.v_schema
			WHERE 
					tab.i_id_table=v_i_id_table
				AND coalesce(idx.v_contype,'p')='p'
				
		ON CONFLICT DO NOTHING
		;

	RETURN	v_i_id_table;
END
$BODY$
LANGUAGE plpgsql 
;

CREATE OR REPLACE FUNCTION sch_repcloud.fn_log_data() 
RETURNS TRIGGER as 
$BODY$
DECLARE
	v_t_sql_insert	text;
	v_t_log_table	text;
	v_i_action_xid	bigint;
	v_old_row		record;
	v_new_row		record;
BEGIN
	v_i_action_xid:=(txid_current()::bigint);

	v_t_log_table:=format('log_%s',TG_RELID);
	IF TG_OP ='INSERT'
	THEN
		v_t_sql_insert:=format(
			'INSERT INTO sch_repnew.%I
			(
				i_xid_action,
				v_action,
				rec_new_data,
				oid_old_tab_oid
				
			)
			VALUES
			(
				%L,
				%L,
				%L,
				%L
			)
			',
			v_t_log_table,
			v_i_action_xid,
			TG_OP,
			NEW,
			TG_RELID
			);
	ELSEIF TG_OP='UPDATE'
	THEN
	v_t_sql_insert:=format(
		'INSERT INTO sch_repnew.%I
		(
			i_xid_action,
			v_action,
			rec_new_data,
			rec_old_data,
			oid_old_tab_oid
		)
		VALUES
		(
			%L,
			%L,
			%L,
			%L,
			%L
		)
		',
		v_t_log_table,
		v_i_action_xid,
		TG_OP,
		NEW,
		OLD,
		TG_RELID
		);
	ELSEIF TG_OP='DELETE'
	THEN
		v_t_sql_insert:=format(
		'INSERT INTO sch_repnew.%I
		(
			i_xid_action,
			v_action,
			rec_old_data,
			oid_old_tab_oid
		)
		VALUES
		(
			%L,
			%L,
			%L,
			%L
		)
		',
		v_t_log_table,
		v_i_action_xid,
		TG_OP,
		OLD,
		TG_RELID
		);
	END IF;
	EXECUTE v_t_sql_insert;
	RETURN NULL;
END
$BODY$
LANGUAGE plpgsql 
;


CREATE OR REPLACE FUNCTION fn_log_truncate() 
RETURNS TRIGGER as 
$BODY$
DECLARE
	v_t_sql_insert	text;
	v_t_log_table	text;
	v_i_action_xid	bigint;
	v_old_row		record;
	v_new_row		record;
BEGIN
	v_t_log_table:=format('%s_%s_log',TG_TABLE_NAME,TG_RELID);
	v_i_action_xid:=(txid_current()::bigint);
		v_t_sql_insert:=format(
		'INSERT INTO sch_repnew.%I
		(
			i_xid_action,
			v_action,
			oid_old_tab_oid
		)
		VALUES
		(
			%L,
			%L,
			%L
		)
		',
		v_t_log_table,
		v_i_action_xid,
		TG_OP,
		TG_RELID
		);
	EXECUTE v_t_sql_insert;
	RETURN NULL;
END
$BODY$
LANGUAGE plpgsql 
;

--VIEWS

CREATE OR REPLACE VIEW v_table_column_types
AS
	SELECT 
		nspname AS schema_name,
		relname AS table_name,
		format('{%s}',col_typ_map)::json AS col_typ_map
		
	FROM
		(
		SELECT 
		string_agg(format(
			'"%s":"%s"',
			replace(attname,'"',''),
			replace(format_type(att.atttypid, att.atttypmod),'"','')),',') AS col_typ_map,
		sch.nspname,
		tab.relname
		
	FROM 
		pg_attribute att 
		INNER JOIN pg_class tab 
			ON tab.oid=att.attrelid
		INNER JOIN pg_catalog.pg_namespace sch
			ON sch.oid=tab.relnamespace
	WHERE 
			attnum>0
		AND NOT attisdropped
	GROUP BY nspname,relname
)
		t_att

;





CREATE OR REPLACE VIEW v_token_idx AS
SELECT 
	v_table,
	v_schema,
	b_indisunique,
	b_idx_constraint,
	v_contype,
	t_idx_token[2] AS t_index_name,
	t_idx_token[4] AS t_index_def,
	t_constraint_def
	
FROM 
( 
	SELECT
		tab.relname AS v_table,
		sch.nspname AS v_schema,
		regexp_match(
			pg_get_indexdef(ind.oid),
			'(CREATE.*INDEX )(.*?)( ON.*? USING.*?)(.*?)'
		) AS t_idx_token,
		CASE 
			WHEN conname IS NULL 
			THEN 
				FALSE
			ELSE 
				TRUE
		END AS b_idx_constraint,
		con.contype AS v_contype,
		CASE 
			WHEN conname IS NOT NULL 
			THEN 
				pg_get_constraintdef(con.oid)
		END AS t_constraint_def,
		idx.indisunique AS b_indisunique
		
	FROM 
		pg_class tab 
		INNER JOIN pg_namespace sch 
			ON sch.oid=tab.relnamespace
		LEFT OUTER JOIN pg_index idx 
			ON idx.indrelid=tab.oid
		LEFT OUTER JOIN pg_class ind 
			ON ind.oid=idx.indexrelid
		LEFT OUTER JOIN pg_constraint con 
			ON	tab.oid=con.conrelid
			AND ind.oid=con.conindid
) idx
;


CREATE OR REPLACE VIEW v_blocking_pids as
SELECT 
	blocked_locks.pid     AS blocked_pid,
	blocked_activity.usename  AS blocked_user,
	blocking_locks.pid     AS blocking_pid,
	blocking_activity.usename AS blocking_user,
	blocked_activity.query    AS blocked_statement,
	(SELECT state from pg_stat_activity where pid=blocking_locks.pid) as blocking_state,
	(SELECT current_timestamp - state_change from pg_stat_activity where pid=blocking_locks.pid) as blocking_age,
	(SELECT query from pg_stat_activity where pid=blocking_locks.pid) as blocking_statement,
	format('SELECT pg_cancel_backend(%L);',blocking_locks.pid ) as cancel_statement,
	format('SELECT pg_terminate_backend(%L);',blocking_locks.pid ) as terminate_statement
 FROM  
	pg_catalog.pg_locks	blocked_locks
	JOIN pg_catalog.pg_stat_activity blocked_activity  
		ON blocked_activity.pid = blocked_locks.pid
	JOIN pg_catalog.pg_locks blocking_locks
		ON	blocking_locks.locktype = blocked_locks.locktype
		AND	blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
		AND	blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
		AND	blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
		AND	blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
		AND	blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
		AND	blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
		AND	blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
		AND	blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
		AND	blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
		AND	blocking_locks.pid != blocked_locks.pid
	JOIN pg_catalog.pg_stat_activity blocking_activity 
		ON blocking_activity.pid = blocking_locks.pid
 WHERE 
	NOT blocked_locks.GRANTED
ORDER BY 
	blocking_age DESC
 ;

CREATE OR REPLACE VIEW v_tab_bloat AS
SELECT
    v_table,
    v_schema,
    rl_tab_tuples,
    i_tab_pages,
    i_pntr_size,
    n_tuple_total_width,
    dbl_nullhdr,
    i_page_hdr,
    n_block_size,
    n_fill_factor,
    round(dbl_tab_min_pages) as dbl_tab_min_pages,
    pg_size_pretty((i_tab_pages*n_block_size)::bigint) as t_tab_size,
    (i_tab_pages*n_block_size::bigint) as i_tab_size,
    i_num_cols,
    round(
        CASE
            WHEN
                    dbl_tab_min_pages=0
                OR  i_tab_pages=0
            THEN
                1.0
        ELSE
            i_tab_pages/dbl_tab_min_pages::numeric
        END,
        1)
    AS n_tab_bloat,
    CASE
        WHEN
            i_tab_pages < dbl_tab_min_pages
        THEN
            0
        ELSE
            round(i_tab_pages::bigint - dbl_tab_min_pages)
    END AS i_tab_wasted_pages,
    CASE
        WHEN
            i_tab_pages < dbl_tab_min_pages
        THEN
            0
        ELSE
            n_block_size*round(i_tab_pages::bigint - dbl_tab_min_pages)
    END AS i_tab_wasted_bytes,
    CASE
        WHEN
            i_tab_pages < dbl_tab_min_pages
        THEN
            pg_size_pretty(0::bigint)
        ELSE
            pg_size_pretty((n_block_size*round(i_tab_pages::bigint - dbl_tab_min_pages))::bigint)
    END AS t_tab_wasted_bytes,
    v_tbs,
	o_tab_oid

FROM
(
    SELECT
        v_table,
        v_schema,
        rl_tab_tuples,
        i_tab_pages,
        i_pntr_size,
        n_tuple_total_width,
        dbl_nullhdr,
        i_page_hdr,
        n_block_size,
        n_fill_factor,
        ceil((rl_tab_tuples*n_tuple_total_width)/((n_block_size-i_page_hdr-dbl_nullhdr)*n_fill_factor)) as dbl_tab_min_pages,
        tab_oid as o_tab_oid,
        i_num_cols,
        v_tbs
    FROM
    (
        SELECT
            v_table,
            v_schema,
            i_avg_width,
            r_null_frac,
            i_hdr,
            i_pntr_size,
            n_block_size,
            i_page_hdr,
            dbl_tuple_width,
            rl_max_null_frac,
            i_nullhdr,
            i_num_cols,
            tab_oid,
            n_fill_factor,
            (t_dat.dbl_tuple_width+(t_dat.i_hdr+t_dat.i_pntr_size-(
                                            CASE
                                                 WHEN
                                                t_dat.i_hdr%t_dat.i_pntr_size=0
                                                 THEN
                                                t_dat.i_pntr_size
                                                 ELSE
                                                t_dat.i_hdr%t_dat.i_pntr_size
                                             END
                                            )
                        )
            )::numeric AS n_tuple_total_width,
            (t_dat.rl_max_null_frac*(t_dat.i_nullhdr+t_dat.i_pntr_size-(
                                              CASE
                                                WHEN
                                                  t_dat.i_nullhdr%t_dat.i_pntr_size=0
                                                THEN
                                                  t_dat.i_pntr_size
                                                ELSE
                                                  t_dat.i_nullhdr%t_dat.i_pntr_size
                                                END
                                            )
                                   )
                        ) AS dbl_nullhdr,
            rl_tab_tuples,
            i_tab_pages,
            v_tbs
        FROM
        (
            SELECT
                    v_table,
                    v_schema,
                    i_avg_width,
                    r_null_frac,
                    i_hdr,
                    i_pntr_size,
                    n_block_size,
                    i_page_hdr,
                    dbl_tuple_width,
                    rl_max_null_frac,
                    CASE
                        WHEN
                            r_null_frac<>0
                        THEN
                            i_hdr+1+i_num_cols/i_pntr_size
                        ELSE
                            0
                    END AS i_nullhdr,
                    i_num_cols,
                    tab_oid,
                    1::numeric as n_fill_factor, --ROUGLY APPROXIMATE 1 FOR ANYTHING
                    rl_tab_tuples,
                    i_tab_pages,
                    CASE
                        WHEN
                            tbs_oid=0
                        THEN
                            (
                                SELECT
                                    spcname
                                FROM
                                    pg_tablespace
                                    WHERE oid=(
                                            SELECT
                                                dattablespace
                                            FROM
                                                pg_database
                                            WHERE
                                                datname=current_database()
                                            )
                            )
                        ELSE
                            (
                                SELECT
                                    spcname
                                FROM
                                    pg_tablespace
                                    WHERE oid=tbs_oid

                            )
                    END
                        as v_tbs

            FROM
            (
                SELECT
                    tab.relname as v_table,
                    sch.nspname as v_schema,
                    sum(sts.avg_width) as i_avg_width,
                    sum(sts.null_frac) as r_null_frac,
                    (SELECT sum(attlen)+5 FROM pg_attribute WHERE attrelid=tab.oid AND attnum<0) as i_hdr,
                    t_vrs.i_pntr_size,
                    t_vrs.n_block_size,
                    t_vrs.i_page_hdr,
                    SUM((1-sts.null_frac)*sts.avg_width) AS dbl_tuple_width,
                    MAX(null_frac) AS rl_max_null_frac,
                    (SELECT count(*) FROM pg_attribute WHERE attrelid=tab.oid AND attnum>0) as i_num_cols,
                    tab.oid as tab_oid,
                    tab.reltuples as rl_tab_tuples,
                    tab.relpages as i_tab_pages,
                    tab.reltablespace as tbs_oid
                FROM
                    pg_class tab
                   	INNER JOIN  pg_namespace sch
                   		ON	tab.relnamespace=sch.oid
                   	LEFT OUTER JOIN pg_stats sts
                   		ON sch.nspname=sts.schemaname
                   		AND sts.tablename=tab.relname,
                   	
                    (
                        SELECT
                        (
                            SELECT current_setting('block_size')::numeric) AS n_block_size,

                            CASE
                                WHEN
                                    substring(t_ver,12,3) IN ('8.0','8.1','8.2')
                                THEN
                                    20
                                WHEN
                                    substring(t_ver,12,3) IN ('8.3','8.4','9.0','9.1','9.2')
                                THEN
                                    24
                                ELSE
                                    24
                            END AS i_page_hdr,
                            CASE
                                WHEN
                                        t_ver ~ 'mingw32'
                                    OR  t_ver ~ '64-bit'
                                THEN
                                    8
                                ELSE
                                    4
                            END AS i_pntr_size,
                            substring(t_ver,12,3)
                        FROM
                        (
                            SELECT
                                version() AS t_ver
                        ) t_version
                    ) t_vrs
                WHERE
                     tab.relkind='r'
                GROUP BY
                    tab.oid,
                    t_vrs.i_pntr_size,
                    t_vrs.n_block_size,
                    t_vrs.i_page_hdr,
                    tab.reltuples,
                    tab.relpages,
                    tab.reltablespace,
                    tab.relname,
                    sch.nspname
            ) t_data
        ) t_dat
    )  t_tab
) t_tab_blt
;

CREATE OR REPLACE VIEW v_create_idx_cons
AS
SELECT 
	CASE
		WHEN b_idx_constraint
		THEN
			CASE 
			WHEN v_contype IN ('p','u')
			THEN
			format(
					'ALTER TABLE sch_repnew.%I ADD CONSTRAINT %s %s;',
					v_new_table_name,
					t_index_name,
					t_constraint_def
				)
			END
		ELSE
			format(
					'CREATE %s INDEX %I ON sch_repnew.%I USING %s;',
					t_unique,
					t_index_name,
					v_new_table_name,
					t_index_def
			)

	END AS t_create,
	v_old_table_name,
	v_new_table_name,
	v_schema_name,
	t_index_name,
	v_contype

FROM
(
SELECT 
	tab.v_old_table_name,
	tab.v_new_table_name,
	tab.v_schema_name,
	idx.v_table_name,
	CASE WHEN	idx.b_indisunique
		THEN 'UNIQUE'
	ELSE ''
	END AS t_unique,
	idx.b_idx_constraint,
	idx.t_index_name,
	idx.t_index_def,
	idx.v_contype,
	idx.t_constraint_def
FROM 
	sch_repcloud.t_idx_repack idx
	INNER JOIN sch_repcloud.t_table_repack tab
		ON tab.i_id_table=idx.i_id_table
) create_idx
;

CREATE OR REPLACE VIEW v_fk_validate
AS
SELECT
	format('ALTER TABLE %I.%I VALIDATE CONSTRAINT %I;',sch.nspname,tab.relname,con.conname) AS t_con_validate,
	sch.nspname as v_schema_name,
	con.conname AS v_con_name,
	tab.relname AS v_table_name
	

FROM
	pg_class tab
	INNER JOIN pg_namespace sch
		ON sch.oid=tab.relnamespace
	INNER JOIN pg_constraint con
		ON
			con.connamespace=tab.relnamespace
		AND	con.conrelid=tab.oid
WHERE
			con.contype in ('f')
		AND NOT con.convalidated
		
;



CREATE OR REPLACE VIEW v_tab_fkeys AS
	SELECT DISTINCT
		format('ALTER TABLE ONLY sch_repnew.%I ADD CONSTRAINT %I %s  NOT VALID ;',rep.v_new_table_name ,conname,pg_get_constraintdef(con.oid)) AS t_con_create,
		format('ALTER TABLE ONLY sch_repnew.%I VALIDATE CONSTRAINT %I ;',rep.v_new_table_name ,conname) AS t_con_validate,
		format('ALTER TABLE ONLY %I.%I DROP CONSTRAINT %I ;',rep.v_schema_name,rep.v_old_table_name ,conname) AS t_con_drop,
		tab.relname as v_table_name,
		sch.nspname as v_schema_name,
		conname AS v_con_name

	FROM
		pg_class tab
		INNER JOIN pg_namespace sch
			ON sch.oid=tab.relnamespace
		INNER JOIN pg_constraint con
			ON
				con.connamespace=tab.relnamespace
			AND	con.conrelid=tab.oid
		INNER JOIN sch_repcloud.t_table_repack rep
			ON tab.oid=rep.oid_old_table 
	WHERE
			con.contype in ('f')
		AND con.confrelid<>con.conrelid
;




CREATE OR REPLACE VIEW v_tab_ref_fkeys AS
	SELECT 
		format('ALTER TABLE ONLY %I.%I ADD CONSTRAINT %I %s sch_repnew.%I %s NOT VALID ;',v_ref_schema_name,v_referencing_table ,v_con_name,t_con_token[1],v_new_ref_table,t_con_token[3]) AS t_con_create,
		CASE WHEN b_self_referencing
		THEN
			'SELECT True;'
		ELSE
			format('ALTER TABLE ONLY %I.%I DROP CONSTRAINT %I ;',v_ref_schema_name,v_referencing_table ,v_con_name) 
		END AS t_con_drop,
		format('ALTER TABLE ONLY  %I.%I VALIDATE CONSTRAINT %I ;',v_ref_schema_name,v_referencing_table,v_con_name) AS t_con_validate,
		format('LOCK TABLE  %I.%I IN ACCESS EXCLUSIVE MODE;',v_ref_schema_name,v_referencing_table,v_con_name) AS t_tab_lock,
		v_old_ref_table,
		v_referenced_schema_name,
		v_schema_name,
		v_con_name,
		v_new_ref_table,
		v_referencing_table,
		v_ref_schema_name
	FROM
		(
			SELECT 
				pg_get_constraintdef(con.oid) AS condef, 
				regexp_match(
					pg_get_constraintdef(con.oid),
					'(FOREIGN KEY\s*\(.*?\)\s*REFERENCES)\s*(.*)(\(.*)'
				) AS t_con_token,
				CASE
					WHEN con.conrelid=con.confrelid
					THEN 
						rep.v_new_table_name
					ELSE 
						tabr.relname
				END AS v_referencing_table,
				CASE
					WHEN con.conrelid=con.confrelid
					THEN 
						'sch_repnew'
					ELSE 
						sch.nspname
				END AS v_ref_schema_name,
				sch.nspname AS v_schema_name,
				con.conname AS v_con_name,
				rep.v_new_table_name AS v_new_ref_table,
				rep.v_old_table_name AS v_old_ref_table,
				rep.v_schema_name AS v_referenced_schema_name,
				con.conrelid=con.confrelid AS b_self_referencing
			
			FROM
			pg_class tab
			INNER JOIN pg_namespace sch
				ON sch.oid=tab.relnamespace
			INNER JOIN pg_constraint con
				ON
					con.connamespace=tab.relnamespace
				AND	con.confrelid=tab.oid
			INNER JOIN sch_repcloud.t_table_repack rep
				 ON con.confrelid=rep.oid_old_table 
			INNER JOIN pg_class tabr
				ON  con.conrelid=tabr.oid
				WHERE
						con.contype in ('f')
					
		) con
;

/*
Views to get dependencies adapted from pgadmin3's query to get referenced objects
*/

CREATE OR REPLACE VIEW v_serials 
AS 
SELECT 
	* 
FROM 
(
	SELECT DISTINCT 
	 	dep.deptype, 
		dep.classid, 
		dep.objid,
		dep.refclassid,
		dep.refobjid,
		ad.adsrc,
		cl.relkind, 
		(SELECT attname FROM pg_catalog.pg_attribute WHERE (attrelid,attnum)=(SELECT adrelid,adnum FROM pg_attrdef WHERE oid=depref.objid)) AS secatt,
		COALESCE(coc.relname, clrw.relname) AS ownertable,
		CASE
			WHEN 
					cl.relname IS NOT NULL 
				AND att.attname IS NOT NULL 
			THEN 
				cl.relname || '.' || att.attname 
			ELSE 
				COALESCE(cl.relname, co.conname, pr.proname, tg.tgname, ty.typname, la.lanname, rw.rulename, ns.nspname) 
		END AS refname,
		COALESCE(nsc.nspname, nso.nspname, nsp.nspname, nst.nspname, nsrw.nspname) AS nspname
	FROM pg_depend dep
		LEFT JOIN pg_class cl 
			ON dep.objid=cl.oid
		LEFT JOIN pg_attribute att 
			ON dep.objid=att.attrelid AND dep.objsubid=att.attnum
		LEFT JOIN pg_namespace nsc 
			ON cl.relnamespace=nsc.oid
		LEFT JOIN pg_proc pr 
			ON dep.objid=pr.oid
		LEFT JOIN pg_namespace nsp 
			ON pr.pronamespace=nsp.oid
		LEFT JOIN pg_trigger tg 
			ON dep.objid=tg.oid
		LEFT JOIN pg_type ty 
			ON dep.objid=ty.oid
		LEFT JOIN pg_namespace nst 
			ON ty.typnamespace=nst.oid
		LEFT JOIN pg_constraint co 
			ON dep.objid=co.oid
		LEFT JOIN pg_class coc 
			ON co.conrelid=coc.oid
		LEFT JOIN pg_namespace nso 
			ON co.connamespace=nso.oid
		LEFT JOIN pg_rewrite rw 
			ON dep.objid=rw.oid
		LEFT JOIN pg_class clrw 
			ON clrw.oid=rw.ev_class
		LEFT JOIN pg_namespace nsrw 
			ON clrw.relnamespace=nsrw.oid
		LEFT JOIN pg_language la 
			ON dep.objid=la.oid
		LEFT JOIN pg_namespace ns 
			ON dep.objid=ns.oid
		LEFT JOIN pg_attrdef ad 
			ON ad.oid=dep.objid
		INNER JOIN pg_catalog.pg_depend depref
			ON dep.objid=depref.refobjid

)ser
WHERE 
		relkind='S'
	AND secatt IS NOT NULL
;


CREATE OR REPLACE VIEW v_get_dep_views
AS
SELECT 
	v_view_name,
	v_schema_name,
	oid_referencing,
	oid_view,
	format('ALTER %s VIEW %I.%I SET SCHEMA sch_repdrop;',t_matview,v_schema_name,v_view_name ) AS t_change_schema,
	format('CREATE %s VIEW %I.%I AS %s %s;',t_matview,v_schema_name,v_view_name,trim(trailing ';' from pg_get_viewdef(oid_view)),t_no_data) AS t_create_view,
	format('DROP %s VIEW %I.%I ;',t_matview,v_schema_name,v_view_name) AS t_drop_view,
	CASE 
	WHEN v_rel_kind='m'
	THEN
		format('REFRESH MATERIALIZED VIEW %I.%I ;',v_schema_name,v_view_name)
	END AS t_refresh_matview
	
FROM
(
	SELECT DISTINCT
		clv.oid AS oid_view,
		dep.refobjid AS oid_referencing,
		clv.relname AS v_view_name,
		nspv.nspname AS v_schema_name,
		clv.relkind AS v_rel_kind,
		CASE WHEN 
			clv.relkind='m'
		THEN	
			'MATERIALIZED'
		ELSE
			''
		END AS t_matview,
		CASE WHEN 
			clv.relkind='m'
		THEN	
			'WITH NO DATA'
		ELSE
			''
		END AS t_no_data
		FROM pg_depend dep
			LEFT JOIN pg_class cl 
				ON dep.objid=cl.oid
			LEFT JOIN pg_attribute att 
				ON dep.objid=att.attrelid AND dep.objsubid=att.attnum
			LEFT JOIN pg_namespace nsc 
				ON cl.relnamespace=nsc.oid
			LEFT JOIN pg_proc pr 
				ON dep.objid=pr.oid
			LEFT JOIN pg_namespace nsp 
				ON pr.pronamespace=nsp.oid
			LEFT JOIN pg_trigger tg 
				ON dep.objid=tg.oid
			LEFT JOIN pg_type ty 
				ON dep.objid=ty.oid
			LEFT JOIN pg_namespace nst 
				ON ty.typnamespace=nst.oid
			LEFT JOIN pg_constraint co 
				ON dep.objid=co.oid
			LEFT JOIN pg_class coc 
				ON co.conrelid=coc.oid
			LEFT JOIN pg_namespace nso 
				ON co.connamespace=nso.oid
			LEFT JOIN pg_rewrite rw 
				ON dep.objid=rw.oid
			LEFT JOIN pg_class clrw 
				ON clrw.oid=rw.ev_class
			LEFT JOIN pg_namespace nsrw 
				ON clrw.relnamespace=nsrw.oid
			LEFT JOIN pg_language la 
				ON dep.objid=la.oid
			LEFT JOIN pg_namespace ns 
				ON dep.objid=ns.oid
			LEFT JOIN pg_attrdef ad 
				ON ad.oid=dep.objid
			INNER JOIN pg_rewrite rev
				ON dep.objid=rev.oid
			INNER JOIN pg_class clv
				ON rev.ev_class=clv.oid
			INNER JOIN pg_namespace nspv
				ON nspv.oid=clv.relnamespace
	WHERE
		dep.classid='pg_rewrite'::regclass

) vdat

;