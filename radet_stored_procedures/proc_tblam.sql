-- PROCEDURE: expanded_radet_client.proc_tblam(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_tblam(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_tblam(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctetblam_partition text;
DECLARE laboratoryresult_partition text;
DECLARE laboratorytest_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctetblam_partition := CONCAT('cte_tblam_',datim_id);
laboratoryresult_partition := CONCAT('laboratory_result_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctetblam_partition);

RAISE NOTICE 'successfully truncate % table', ctetblam_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT * 
FROM (SELECT lr.id, lr.patient_uuid as personuuidtblam,lr.ods_datim_id odsdatimidtblam,
CAST(lr.date_result_reported AS DATE) AS dateOfLastTbLam, 
lr.result_reported as tbLamResult, 
ROW_NUMBER () OVER (PARTITION BY lr.patient_uuid ORDER BY lr.date_result_reported DESC) as rank2333 
FROM public.%I lr 
INNER JOIN public.%I lt on lr.test_id = lt.id 
WHERE lt.lab_test_id = 51 AND lr.date_result_reported IS NOT NULL 
AND lr.date_result_reported <=  %L
AND lr.date_result_reported >= ''1980-01-01''
AND lr.result_reported is NOT NULL) as tblam 
WHERE tblam.rank2333 = 1
',ctetblam_partition,laboratoryresult_partition,
laboratorytest_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctetblam_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_tblam(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_tblam('tZy8wIM53xT');