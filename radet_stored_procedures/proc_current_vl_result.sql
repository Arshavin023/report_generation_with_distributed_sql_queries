-- PROCEDURE: expanded_radet_client.proc_current_vl_result(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_current_vl_result(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_current_vl_result(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctecurrentvlresult_partition text;
DECLARE laboratoryresult_partition text;
DECLARE laboratorytest_partition text;
DECLARE laboratorysample_partition text;
DECLARE baseappcodeset_partition text;
DECLARE period_end_date DATE;
DECLARE period_text TEXT;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT periodcode INTO period_text
FROM expanded_radet.period WHERE is_active;

-- Determine period_end_date based on period_text
IF position('Q' IN period_text) > 0 THEN
	SELECT (date + INTERVAL '1 MONTH')::DATE
	INTO period_end_date
	FROM expanded_radet.period WHERE is_active;
ELSE
	SELECT date::DATE
	INTO period_end_date 
	FROM expanded_radet.period WHERE is_active;
END IF;

ctecurrentvlresult_partition := CONCAT('cte_current_vl_result_',datim_id);
laboratoryresult_partition := CONCAT('laboratory_result_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
laboratorysample_partition := CONCAT('laboratory_sample_',datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctecurrentvlresult_partition);

RAISE NOTICE 'successfully truncate % table', ctecurrentvlresult_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT * FROM (SELECT sm.patient_uuid as person_uuid130,sm.ods_datim_id cvl_ods_datim_id,
CAST(ls.date_sample_collected AS DATE ) AS dateOfCurrentViralLoadSample,  
sm.facility_id as vlFacility, sm.archived as vlArchived, 
acode.display as viralLoadIndication, 
sm.result_reported as currentViralLoad,
CAST(sm.date_result_reported AS DATE) as dateOfCurrentViralLoad,
ROW_NUMBER () OVER (PARTITION BY sm.patient_uuid ORDER BY sm.date_result_reported DESC) as rank2
FROM public.%I  sm
INNER JOIN public.%I  lt on sm.test_id = lt.id AND sm.patient_uuid=lt.patient_uuid
INNER JOIN public.%I ls on ls.test_id = lt.id AND ls.patient_uuid=lt.patient_uuid
INNER JOIN public.%I  acode on acode.id =  lt.viral_load_indication
WHERE lt.lab_test_id = 16
AND  lt.viral_load_indication !=719
AND sm. date_result_reported IS NOT NULL
AND sm.date_result_reported <= %L
AND sm.result_reported is NOT NULL
)as vl_result
WHERE vl_result.rank2 = 1
AND (vl_result.vlArchived = 0 OR vl_result.vlArchived is null)
',ctecurrentvlresult_partition,laboratoryresult_partition,
laboratorytest_partition,laboratorysample_partition,
baseappcodeset_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctecurrentvlresult_partition,start_time,end_time,datim_id);


END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_current_vl_result(character varying)
    OWNER TO lamisplus_etl;
	
-- CALL expanded_radet_client.proc_current_vl_result('tZy8wIM53xT');
