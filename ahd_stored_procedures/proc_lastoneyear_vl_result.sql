-- PROCEDURE: ahd.proc_lastoneyear_vl_result(character varying)

-- DROP PROCEDURE IF EXISTS ahd.proc_lastoneyear_vl_result(character varying);

CREATE OR REPLACE PROCEDURE ahd.proc_lastoneyear_vl_result(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctelastoneyearvlresult_partition text;
DECLARE laboratoryresult_partition text;
DECLARE laboratorytest_partition text;
DECLARE laboratorysample_partition text;
DECLARE baseappcodeset_partition text;
DECLARE period_end_date DATE;
DECLARE period_text TEXT;
DECLARE ahdmonitoringpartition text;

BEGIN
SELECT periodcode 
INTO period_text
FROM ahd.period WHERE is_active;

SELECT CAST(date AS DATE) 
INTO period_end_date
FROM ahd.period WHERE is_active;

ctelastoneyearvlresult_partition := CONCAT('cte_lastoneyear_vl_result_',datim_id);
laboratoryresult_partition := CONCAT('laboratory_result_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
laboratorysample_partition := CONCAT('laboratory_sample_',datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_',datim_id);
ahdmonitoringpartition := CONCAT('ahd_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE ahd.%I',ctelastoneyearvlresult_partition);

RAISE NOTICE 'successfully truncate % table', ctelastoneyearvlresult_partition;

EXECUTE FORMAT('INSERT INTO ahd.%I
SELECT * FROM (SELECT sm.patient_uuid as lastoneyear_person_uuid,
sm.ods_datim_id lastoneyear_ods_datim_id,
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
AND sm.date_result_reported >= (%L::date - INTERVAL ''364 DAY'')
AND sm.date_result_reported <= %L
AND sm.result_reported is NOT NULL
)as vl_result
WHERE (vl_result.vlArchived = 0 OR vl_result.vlArchived is null)
--AND vl_result.rank2 = 1 
',ctelastoneyearvlresult_partition,laboratoryresult_partition,
laboratorytest_partition,laboratorysample_partition,
baseappcodeset_partition,period_end_date,period_end_date);

SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO ahd.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
ahdmonitoringpartition,ctelastoneyearvlresult_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE ahd.proc_lastoneyear_vl_result(character varying)
    OWNER TO lamisplus_etl;

-- CALL ahd.proc_lastoneyear_vl_result('A1xxdELs2fm');
-- SELECT * FROM ahd.ahd_monitoring;
