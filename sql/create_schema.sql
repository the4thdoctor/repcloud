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
	--t_old_table:=format('%I.%I',p_t_schema,p_t_table);
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
	INSERT INTO sch_repcloud.t_table_repack (v_old_table_name,v_new_table_name,v_schema_name)
		VALUES (p_t_table,v_new_table,p_t_schema) ON CONFLICT DO NOTHING;
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
    v_tbs

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
        tab_oid,
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

CREATE OR REPLACE VIEW v_idx_bloat AS
SELECT DISTINCT ON 	(
			v_schema,
			v_table,
			v_idx
		)
		v_schema,
		v_table,
		v_idx,
		i_idx_size,
		pg_size_pretty(i_idx_size) as t_idx_size,
		rl_idx_tuples,
		i_idx_pages,
		dbl_idx_min_pages,
		n_idx_bloat,
		n_idx_bloat_raw,
		i_idx_wasted_pages,
		i_idx_wasted_bytes,
		t_idx_wasted_size,
		i_num_cols,
		n_fill_factor,
		CASE
			WHEN b_is_pkey
			THEN
				'-- CANNOT REINDEX PRIMARY KEYS CONCURRENTLY'
			ELSE
				format('%s CONCURRENTLY %I %s;',
					v_reindex[1], --create index`
					v_reindex[2]::character varying(30)||idx_id||'_new', -- index name
					v_reindex[3] -- ON TABLE clause
				)
				||
				format('BEGIN;ALTER INDEX %I.%I RENAME TO %I;',
					v_schema,
					v_reindex[2],
					v_reindex[2]::character varying(30)||idx_id||'_old'
				)
				||
				format('ALTER INDEX %I.%I RENAME TO %I;',
					v_schema,
					v_reindex[2]::character varying(30)||idx_id||'_new',
					v_reindex[2]
				)
				||
				format('DROP INDEX %I.%I;COMMIT;',
					v_schema,
					v_reindex[2]::character varying(30)||idx_id||'_old'
				)

		END AS v_reindex,
		'REINDEX INDEX '||v_schema||'.'||v_idx||';' AS v_sql_reindex,
		b_is_pkey,
		b_is_uk,
		v_constraint,
		t_idx_cols,
		v_tbs,
		b_idx_simple,
		oid_idx

FROM
	(
		SELECT
			v_schema,
			tab.relname as v_table,
			EXTRACT(EPOCH FROM now())::bigint::text as idx_id,
			v_idx,
			rl_idx_tuples,
			pg_relation_size(format('%I.%I',v_schema,v_idx)::regclass) as i_idx_size,
			i_idx_pages,
					regexp_match(
						pg_get_indexdef(format('%I.%I',v_schema,v_idx)::regclass),
						'(CREATE.*INDEX )(.*?)( ON.*)'
					) as v_reindex,
			round(dbl_idx_min_pages) as dbl_idx_min_pages,
			round(
				CASE
					WHEN
							dbl_idx_min_pages=0
						OR 	i_idx_pages=0
					THEN
						0.0
				ELSE
					i_idx_pages/dbl_idx_min_pages::numeric
				END,1
			) AS n_idx_bloat,
			CASE
				WHEN
						dbl_idx_min_pages=0
					OR 	i_idx_pages=0
				THEN
					0.0
				ELSE i_idx_pages/dbl_idx_min_pages::numeric
			END AS n_idx_bloat_raw,
			CASE
				WHEN
					i_idx_pages < dbl_idx_min_pages
				THEN
					0
				ELSE
					round(i_idx_pages::bigint - dbl_idx_min_pages)
			END AS i_idx_wasted_pages,
			CASE
				WHEN
					i_idx_pages < dbl_idx_min_pages
				THEN
					0
				ELSE
					n_block_size*round(i_idx_pages::bigint - dbl_idx_min_pages)
				END
			AS i_idx_wasted_bytes,
			CASE
				WHEN
					i_idx_pages < dbl_idx_min_pages
				THEN
					pg_size_pretty(0::bigint)
				ELSE
					pg_size_pretty((n_block_size*round(i_idx_pages-dbl_idx_min_pages))::bigint)
				END
			AS t_idx_wasted_size,
			i_num_cols,
			n_fill_factor,
			ind.indisprimary as b_is_pkey,
			ind.indisunique as b_is_uk,
			cons.conname as v_constraint,
			(
				SELECT
					array_to_string(array_agg(quote_ident(attname)),',')
				FROM
					pg_attribute
				WHERE
						attrelid=ind.indexrelid



			) as t_idx_cols,
			v_tbs,
			(ind.indexprs IS NULL) AS b_idx_simple,
			ind.indexrelid as oid_idx
		FROM
		(
			SELECT
				v_schema,
				v_idx,
				rl_idx_tuples,
				i_idx_pages,
				i_pntr_size,
				n_tuple_total_width,
				n_block_size,
				n_fill_factor,
				(
					(
						coalesce(
							ceil((rl_idx_tuples*(n_tuple_total_width-12))/(n_block_size-i_page_hdr::float))
							,0
							)
					)*n_fill_factor

				)+1 AS dbl_idx_min_pages,
				t_idx.idx_oid,
				i_num_cols,
				v_tbs
			FROM
			(
				SELECT
					t_dat.idx_oid,
					t_dat.v_schema,
					t_dat.v_idx,
					idx.reltuples as rl_idx_tuples,
					idx.relpages as i_idx_pages,
					t_dat.i_pntr_size,
					t_dat.n_block_size,
					(
						t_dat.dbl_tuple_width+(
									t_dat.i_hdr+t_dat.i_pntr_size-(
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
					(
						t_dat.rl_max_null_frac*(
									t_dat.i_nullhdr+t_dat.i_pntr_size-(
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
					t_dat.i_page_hdr,
					i_num_cols,
					--DEFAULT FILLFACTOR FOR INDEXES IS 90% FOR LEAF PAGES ~ 1.4
					1.4::numeric n_fill_factor,
					CASE
						WHEN
							idx.reltablespace =0
						THEN
						(
							SELECT
								spcname
							FROM
								pg_tablespace
							WHERE
								oid=(
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
							WHERE
								oid=idx.reltablespace

						)
					END
					AS v_tbs

				FROM
				(
					SELECT
						idx.v_idx,
						idx.v_schema,
						sum(sts.avg_width) as i_avg_width,
						sts.null_frac,
						t_vrs.i_hdr,
						t_vrs.i_pntr_size,
						t_vrs.n_block_size,
						t_vrs.i_page_hdr,
						SUM((1-sts.null_frac)*sts.avg_width) AS dbl_tuple_width,
						MAX(null_frac) AS rl_max_null_frac,
						CASE
							WHEN
								sts.null_frac<>0
							THEN
								t_vrs.i_hdr+1+count(*)/8
							ELSE
								0
						END AS i_nullhdr,
						count(*) as i_num_cols,
						idx.idx_oid

					FROM
					(
						SELECT
							v_idx,
							v_table,
							v_schema,
							col.attname as v_idx_key,
							idx_oid

						FROM
						(
							SELECT
								unnest(string_to_array(ind.indkey::text,' '))::smallint as i_idx_keys,
								ind.indexrelid,
								ind.indrelid,
								idx.relname as v_idx,
								tab.relname as v_table,
								sch.nspname as v_schema,
								tab.oid,
								idx.oid as idx_oid
							FROM
								pg_index ind,
								pg_class idx,
								pg_class tab,
								pg_namespace sch
							WHERE
									ind.indexrelid=idx.oid
								AND	ind.indrelid=tab.oid
								AND	idx.relnamespace=sch.oid
								AND	tab.relnamespace=sch.oid

						) t_idx,
						  pg_attribute col
						WHERE
								t_idx.i_idx_keys=col.attnum
							AND	col.attrelid=t_idx.oid
					) idx,
					pg_stats sts,
					(
						SELECT
							(SELECT current_setting('block_size')::numeric) AS n_block_size,
							CASE
								WHEN
									substring(t_ver,12,3) IN ('8.0','8.1','8.2')
								THEN
									27
								ELSE
									23
							END AS i_hdr,
							CASE
								WHEN
									substring(t_ver,12,3) IN ('8.0','8.1','8.2')
								THEN
									20
								ELSE
									24
							END AS i_page_hdr,
							CASE
								WHEN
										t_ver ~ 'mingw32'
									OR 	t_ver ~ '64-bit'
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
							idx.v_table=sts.tablename
						AND	idx.v_schema=sts.schemaname
						AND	idx.v_idx_key=sts.attname

					GROUP BY
						idx.idx_oid,
						idx.v_idx,
						idx.v_schema,
						sts.null_frac,
						t_vrs.i_hdr,
						t_vrs.i_pntr_size,
						t_vrs.n_block_size,
						t_vrs.i_page_hdr
					ORDER BY v_idx
				) t_dat,
				  pg_class idx
				WHERE
					t_dat.idx_oid = idx.oid
			) t_idx
		) t_bloat,
		pg_index ind
			LEFT OUTER JOIN pg_constraint cons
			ON
				cons.conindid=ind.indexrelid
		,
		pg_class tab
		WHERE
				idx_oid=ind.indexrelid
			AND	ind.indrelid=tab.oid

		ORDER BY i_idx_wasted_bytes DESC
	) t_idx_stat
;

