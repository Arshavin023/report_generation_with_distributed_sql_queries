-- PROCEDURE: expanded_radet_client.proc_sample_collection_date(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_sample_collection_date(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_sample_collection_date(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctesamplecollectiondate_partition text;
DECLARE laboratorytest_partition text;
DECLARE laboratorysample_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;

ctesamplecollectiondate_partition := CONCAT('cte_sample_collection_date_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
laboratorysample_partition := CONCAT('laboratory_sample_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctesamplecollectiondate_partition);

RAISE NOTICE 'successfully truncate % table', ctesamplecollectiondate_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT patient_uuid as personsamplecd,ods_datim_id sampd_ods_datim_id,
CAST(sample.date_sample_collected AS DATE) as dateofviralloadsamplecollection
FROM (SELECT sm.date_sample_collected, sm.patient_uuid,sm.ods_datim_id,
ROW_NUMBER () OVER (PARTITION BY sm.patient_uuid ORDER BY date_sample_collected DESC) as rnkk
FROM public.%I  sm
INNER JOIN public.%I lt ON lt.id = sm.test_id AND lt.patient_uuid=sm.patient_uuid
WHERE lt.lab_test_id=16 
AND lt.viral_load_indication !=719
AND sm.date_sample_collected IS NOT null
AND sm. date_sample_collected <= %L
AND (sm.archived is null OR sm.archived = 0)
) as sample
WHERE sample.rnkk = 1',
ctesamplecollectiondate_partition,laboratorysample_partition,
laboratorytest_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctesamplecollectiondate_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_sample_collection_date(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_sample_collection_date('tZy8wIM53xT');