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
	oid_old_table oid,
	oid_new_table oid,
	v_old_table_name character varying(100) NOT NULL,
	v_new_table_name  character varying(100) NOT NULL,
	v_schema_name character varying(100) NOT NULL,
	t_create_identiy text,
	t_create_idx text[],
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

CREATE TABLE sch_repcloud.t_idx_repack (
	i_id_index	bigserial,
	i_id_table	bigint NOT NULL,
	v_table_name character varying(100) NOT NULL,
	v_schema_name character varying(100) NOT NULL,
	b_indisunique bool NULL,
	b_idx_constraint bool NULL,
	v_contype char(1) NULL,
	t_index_name text NOT NULL,
	t_index_def text NOT NULL,
	CONSTRAINT pk_t_idx_repack PRIMARY KEY (i_id_index)
);

CREATE UNIQUE INDEX uidx_t_idx_repack_table_schema_INDEX ON t_idx_repack(v_schema_name,v_table_name,t_index_name);



CREATE OR REPLACE FUNCTION fn_create_repack_table(text,text) 
RETURNS character varying(64) as 
$BODY$
DECLARE
	p_t_schema			ALIAS FOR $1;
	p_t_table				ALIAS FOR $2;
	v_new_table			character varying(64);
	v_i_id_table			bigint;
	t_sql_create 		text;
	v_oid_old_table	oid;
	v_oid_new_table	oid;
BEGIN
	v_oid_old_table:=format('%I.%I',p_t_schema,p_t_table)::regclass::oid;
	v_new_table:=format('%I',p_t_table::character varying(30)||'_'||v_oid_old_table::text);
	t_sql_create:=format('
		CREATE TABLE IF NOT EXISTS sch_repcloud.%s
			(LIKE %I.%I)',
			v_new_table,
			p_t_schema,
			p_t_table
			
		);
	EXECUTE t_sql_create ;
	v_oid_new_table:=format('sch_repcloud.%I',v_new_table)::regclass::oid;
	INSERT INTO sch_repcloud.t_table_repack 
		(
			oid_old_table,
			v_old_table_name,
			oid_new_table,
			v_new_table_name,
			v_schema_name
		)
		VALUES 
			(
				v_oid_old_table,
				p_t_table,
				v_oid_new_table,
				v_new_table,
				p_t_schema
			) 
		ON CONFLICT (v_schema_name,v_old_table_name)
			DO UPDATE 
				SET 
					v_new_table_name=v_new_table
	;	
	RETURN v_new_table;
	
END
$BODY$
LANGUAGE plpgsql 
;


--VIEWS

CREATE OR REPLACE VIEW v_token_idx AS
SELECT 
	v_table,
	v_schema,
	b_indisunique,
	b_idx_constraint,
	v_contype,
	t_idx_token[2] AS t_index_name,
	t_idx_token[4] AS t_index_def
	
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
                    pg_class tab,
                    pg_namespace sch,
                    pg_stats sts,
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
                        sts.tablename=tab.relname
                    AND tab.relnamespace=sch.oid
                    AND sch.nspname=sts.schemaname
										AND tab.relkind='r'
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
