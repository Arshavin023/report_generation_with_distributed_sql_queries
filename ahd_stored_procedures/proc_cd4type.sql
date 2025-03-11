-- PROCEDURE: ahd.proc_cd4type(character varying)

-- DROP PROCEDURE IF EXISTS ahd.proc_cd4type(character varying);

CREATE OR REPLACE PROCEDURE ahd.proc_cd4type(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctecd4type_partition text;
DECLARE hivartclinical_partition text;
DECLARE period_end_date DATE;
DECLARE ahdmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM ahd.period WHERE is_active;

ctecd4type_partition := CONCAT('cte_cd4type_',datim_id);
hivartclinical_partition := CONCAT('hiv_art_clinical_',datim_id);
ahdmonitoringpartition := CONCAT('ahd_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE ahd.%I', ctecd4type_partition);

RAISE NOTICE 'successfully truncate % table', ctecd4type_partition;

EXECUTE FORMAT('INSERT INTO ahd.%I
SELECT person_uuid cd4type_person_uuid, cd4_type, ods_datim_id cd4type_ods_datim_id
FROM (SELECT person_uuid, ods_datim_id, visit_date, cd4_type,
ROW_NUMBER() OVER (PARTITION BY person_uuid ORDER BY visit_date DESC) AS rnkk
FROM public.%I 
WHERE archived=0 AND visit_date <= %L) sub
WHERE sub.rnkk = 1',
ctecd4type_partition,hivartclinical_partition,period_end_date);

SELECT TIMEOFDAY() INTO end_time;

EXECUTE FORMAT('INSERT INTO ahd.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
ahdmonitoringpartition,ctecd4type_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE ahd.proc_cd4type(character varying)
    OWNER TO lamisplus_etl;

-- CALL ahd.proc_cd4type('A1xxdELs2fm');
-- SELECT * FROM ahd.ahd_monitoring;