-- PROCEDURE: expanded_radet_client.proc_labcd4(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_labcd4(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_labcd4(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctelabcd4_partition text;
DECLARE laboratoryresult_partition text;
DECLARE laboratorytest_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctelabcd4_partition := CONCAT('cte_labcd4_',datim_id);
laboratoryresult_partition := CONCAT('laboratory_result_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctelabcd4_partition);

RAISE NOTICE 'successfully truncate % table', ctelabcd4_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT * FROM (
SELECT sm.patient_uuid AS cd4_person_uuid,  sm.ods_datim_id lab_ods_datim_id,
sm.result_reported as cd4Lb,sm.date_result_reported as dateOfCD4Lb, 
ROW_NUMBER () OVER (PARTITION BY sm.patient_uuid ORDER BY date_result_reported DESC) as rnk
FROM public.%I  sm
INNER JOIN public.%I  lt on sm.test_id = lt.id AND sm.patient_uuid=lt.patient_uuid
WHERE lt.lab_test_id IN (1, 50) 
AND sm. date_result_reported IS NOT NULL
AND sm.archived = 0
AND sm.date_result_reported <= %L)as cd4_result
WHERE  cd4_result.rnk = 1
',ctelabcd4_partition,laboratoryresult_partition,
laboratorytest_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctelabcd4_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_labcd4(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_labcd4('tZy8wIM53xT');