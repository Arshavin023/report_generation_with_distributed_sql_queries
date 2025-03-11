-- PROCEDURE: expanded_radet_client.proc_tb_sample_collection(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_tb_sample_collection(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_tb_sample_collection(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctetbsamplecollection_partition text;
DECLARE laboratorysample_partition text;
DECLARE laboratorytest_partition text;
DECLARE laboratorylabtest_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctetbsamplecollection_partition := CONCAT('cte_tb_sample_collection_',datim_id);
laboratorysample_partition := CONCAT('laboratory_sample_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
laboratorylabtest_partition := CONCAT('laboratory_labtest_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctetbsamplecollection_partition);

RAISE NOTICE 'successfully truncate % table', ctetbsamplecollection_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT sample.patient_uuid as personTbSample,sample.ods_datim_id tbsamp_ods_datim_id,
sample.created_by,CAST(sample.date_sample_collected AS DATE) as dateOfTbSampleCollection
FROM (SELECT llt.lab_test_name,sm.created_by,sm.ods_datim_id, lt.viral_load_indication, 
sm.facility_id,sm.date_sample_collected, sm.patient_uuid, sm.archived, 
ROW_NUMBER () OVER (PARTITION BY sm.patient_uuid ORDER BY date_sample_collected DESC) as rnkk
FROM public.%I  sm
INNER JOIN public.%I lt ON lt.id = sm.test_id AND lt.patient_uuid=sm.patient_uuid
INNER JOIN  public.%I llt on llt.id = lt.lab_test_id
WHERE lt.lab_test_id IN (65, 66, 51, 64, 67, 72, 71, 86, 58, 73)
AND sm.archived = 0
AND sm.date_sample_collected <= %L
)as sample
WHERE sample.rnkk = 1
',ctetbsamplecollection_partition,laboratorysample_partition,
laboratorytest_partition,laboratorylabtest_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctetbsamplecollection_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_tb_sample_collection(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_tb_sample_collection('tZy8wIM53xT');