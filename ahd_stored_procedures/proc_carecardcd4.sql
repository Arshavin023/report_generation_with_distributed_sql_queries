-- PROCEDURE: ahd.proc_carecardcd4(character varying)

-- DROP PROCEDURE IF EXISTS ahd.proc_carecardcd4(character varying);

CREATE OR REPLACE PROCEDURE ahd.proc_carecardcd4(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctecareCardCD4_partition text;
DECLARE hivartclinical_partition text;
DECLARE period_end_date DATE;
DECLARE ahdmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM ahd.period WHERE is_active;

ctecareCardCD4_partition := CONCAT('cte_carecardcd4_',datim_id);
hivartclinical_partition := CONCAT('hiv_art_clinical_',datim_id);
ahdmonitoringpartition := CONCAT('ahd_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE ahd.%I', ctecareCardCD4_partition);

RAISE NOTICE 'successfully truncate % table', ctecareCardCD4_partition;

EXECUTE FORMAT('INSERT INTO ahd.%I
SELECT person_uuid AS cccd4_person_uuid,ods_datim_id ccd4_ods_datim_id, visit_date,
coalesce(cast(cd_4 as varchar), cd4_semi_quantitative) as cd_4
FROM public.%I
WHERE is_commencement is true
AND  archived = 0 AND  cd_4 != 0 AND visit_date <= %L
',ctecareCardCD4_partition,hivartclinical_partition,period_end_date);

SELECT TIMEOFDAY() INTO end_time;

EXECUTE FORMAT('INSERT INTO ahd.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
ahdmonitoringpartition,ctecareCardCD4_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE ahd.proc_carecardcd4(character varying)
    OWNER TO lamisplus_etl;

-- CALL ahd.proc_carecardcd4('A1xxdELs2fm');