-- PROCEDURE: ahd.proc_lastlflam(character varying)

-- DROP PROCEDURE IF EXISTS ahd.proc_lastlflam(character varying);

CREATE OR REPLACE PROCEDURE ahd.proc_lastlflam(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctelastlflam_partition text;
DECLARE laboratoryresult_partition text;
DECLARE laboratorytest_partition text;
DECLARE period_end_date DATE;
DECLARE period_text TEXT;
DECLARE ahdmonitoringpartition text;

BEGIN
SELECT periodcode INTO period_text
FROM ahd.period WHERE is_active;

SELECT date::DATE
INTO period_end_date 
FROM ahd.period WHERE is_active;

ctelastlflam_partition := CONCAT('cte_lastlflam_',datim_id);
laboratoryresult_partition := CONCAT('laboratory_result_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
ahdmonitoringpartition := CONCAT('ahd_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE ahd.%I',ctelastlflam_partition);

RAISE NOTICE 'successfully truncate % table', ctelastlflam_partition;

EXECUTE FORMAT('INSERT INTO ahd.%I
SELECT personuuid12 lastlflam_personuuid,date_result_reported AS dateOflastlflam, 
result_reported AS lastlflam, ods_datim_id lastlflam_partition_datim_id
FROM (SELECT lr.patient_uuid AS personuuid12,lr.date_result_reported,lr.result_reported,
lt.ods_datim_id,
ROW_NUMBER() OVER (PARTITION BY lr.patient_uuid ORDER BY lr.date_result_reported DESC) AS rowNum 
FROM public.%I lt
INNER JOIN public.%I  lr ON lr.test_id = lt.id  AND lt.patient_uuid=lr.patient_uuid
WHERE lt.lab_test_id = 71 AND lr.date_result_reported <= %L) dt
WHERE dt.rowNum = 1',
ctelastlflam_partition,laboratorytest_partition,
laboratoryresult_partition,period_end_date);

SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO ahd.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
ahdmonitoringpartition,ctelastlflam_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE ahd.proc_lastlflam(character varying)
    OWNER TO lamisplus_etl;
	
-- CALL ahd.proc_lastlflam('A1xxdELs2fm');
-- SELECT * FROM ahd.ahd_monitoring;