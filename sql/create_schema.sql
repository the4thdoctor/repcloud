--CREATE SCHEMA
CREATE SCHEMA IF NOT EXISTS sch_repcloud;

SET search_path=sch_repcloud;
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
	oid_table oid,
	v_old_table_name character varying(100) NOT NULL,
	v_new_table_name  character varying(100) NOT NULL,
	v_schema_name character varying(100) NOT NULL,
	t_create_identiy text,
	t_create_iidx text[],
	v_repack_step character varying(100),
	v_status  character varying(100) ,
	i_size_start bigint,
	i_size_end bigint,
	ts_repack_start	timestamp without time zone,
	ts_repack_end	timestamp without time zone,
	CONSTRAINT pk_t_table_repack PRIMARY KEY (i_id_table)
)
;

CREATE UNIQUE INDEX uidx_t_table_repack_table_schema ON t_table_repack(v_schema_name,v_old_table_name);


CREATE OR REPLACE FUNCTION fn_create_repack_table(text,text) 
RETURNS VOID as 
$BODY$
DECLARE
	p_t_schema			ALIAS FOR $1;
	p_t_table				ALIAS FOR $2;
	v_new_table			character varying(64);
	t_sql_create 		text;
	oid_old_table		oid;
BEGIN
	oid_old_table:=format('%I.%I',p_t_schema,p_t_table)::regclass::oid;
	v_new_table:=format('%I',p_t_table::character varying(30)||'_'||oid_old_table::text);
	t_sql_create:=format('
		CREATE TABLE IF NOT EXISTS sch_repcloud.%s
			(LIKE %I.%I)',
			v_new_table,
			p_t_schema,
			p_t_table
			
		);
	EXECUTE t_sql_create ;
	INSERT INTO sch_repcloud.t_table_repack (oid_table,v_old_table_name,v_new_table_name,v_schema_name)
		VALUES (oid_old_table,p_t_table,v_new_table,p_t_schema) 
		ON CONFLICT (v_schema_name,v_old_table_name)
			DO UPDATE 
				SET 
					v_new_table_name=v_new_table
					
			;
		
		
END
$BODY$
LANGUAGE plpgsql 
;


--VIEWS

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
