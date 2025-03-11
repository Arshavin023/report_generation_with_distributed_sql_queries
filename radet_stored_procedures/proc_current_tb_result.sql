-- PROCEDURE: expanded_radet_client.proc_current_tb_result(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_current_tb_result(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_current_tb_result(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctecurrenttbresult_partition text;
DECLARE laboratoryresult_partition text;
DECLARE laboratorytest_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctecurrenttbresult_partition := CONCAT('cte_current_tb_result_',datim_id);
laboratoryresult_partition := CONCAT('laboratory_result_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctecurrenttbresult_partition);

RAISE NOTICE 'successfully truncate % table', ctecurrenttbresult_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
WITH tb_test as (SELECT personTbResult, curr_ods_datim_id, dateofTbDiagnosticResultReceived,
coalesce(
MAX(CASE WHEN lab_test_id = 65 THEN tbDiagnosticResult END),
MAX(CASE WHEN lab_test_id = 66 THEN tbDiagnosticResult END),
MAX(CASE WHEN lab_test_id = 51 THEN tbDiagnosticResult END),
MAX(CASE WHEN lab_test_id = 64 THEN tbDiagnosticResult END),
MAX(CASE WHEN lab_test_id = 67 THEN tbDiagnosticResult END),
MAX(CASE WHEN lab_test_id = 72 THEN tbDiagnosticResult END),
MAX(CASE WHEN lab_test_id = 71 THEN tbDiagnosticResult END),
MAX(CASE WHEN lab_test_id = 86 THEN tbDiagnosticResult END),
MAX(CASE WHEN lab_test_id = 73 THEN tbDiagnosticResult END),
MAX(CASE WHEN lab_test_id = 58 THEN tbDiagnosticResult END)
) as tbDiagnosticResult,
coalesce(
MAX(CASE WHEN lab_test_id = 65 THEN ''Gene Xpert'' END) ,
MAX(CASE WHEN lab_test_id = 66 THEN ''Chest X-ray'' END) ,
MAX(CASE WHEN lab_test_id = 51 THEN ''TB-LAM'' END) ,
MAX(CASE WHEN lab_test_id = 64 THEN ''AFB Smear Microscopy'' END),
MAX(CASE WHEN lab_test_id = 67 THEN ''Gene Xpert'' END) ,
MAX(CASE WHEN lab_test_id = 72 THEN ''TrueNAT'' END) ,
MAX(CASE WHEN lab_test_id = 71 THEN ''LF-LAM'' END) ,
MAX(CASE WHEN lab_test_id = 86 THEN ''Cobas'' END) ,
MAX(CASE WHEN lab_test_id = 73 THEN ''TB LAMP'' END) ,
MAX(CASE WHEN lab_test_id = 58 THEN ''TB-LAM'' END)
) as tbDiagnosticTestType

FROM (SELECT sm.patient_uuid as personTbResult, sm.ods_datim_id curr_ods_datim_id,sm.result_reported as tbDiagnosticResult,
CAST(sm.date_result_reported AS DATE) as dateofTbDiagnosticResultReceived,
lt.lab_test_id
FROM public.%I  sm
INNER JOIN public.%I  lt on sm.test_id = lt.id AND sm.patient_uuid=lt.patient_uuid
WHERE lt.lab_test_id IN (65, 51, 64, 67, 72, 71, 86, 58, 73, 66) 
and sm.archived = 0
AND sm.date_result_reported is not null
AND sm.date_result_reported <= %L
) as dt
GROUP BY dt.personTbResult, dt.curr_ods_datim_id,dt.dateofTbDiagnosticResultReceived)
select * from (select *, row_number() over (partition by personTbResult
order by dateofTbDiagnosticResultReceived desc ) as rnk from tb_test) as dt
where rnk = 1
',ctecurrenttbresult_partition,
laboratoryresult_partition,laboratorytest_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctecurrenttbresult_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_current_tb_result(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_current_tb_result('tZy8wIM53xT');