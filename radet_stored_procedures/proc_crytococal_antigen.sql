-- PROCEDURE: expanded_radet_client.proc_crytococal_antigen(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_crytococal_antigen(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_crytococal_antigen(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctecrytococalantigen_partition text;
DECLARE laboratorytest_partition text;
DECLARE laboratoryresult_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active; ------?2

ctecrytococalantigen_partition := CONCAT('cte_crytococal_antigen_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
laboratoryresult_partition := CONCAT('laboratory_result_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctecrytococalantigen_partition);

RAISE NOTICE 'successfully truncate % table', ctecrytococalantigen_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
select *
from (
select DISTINCT ON (lr.patient_uuid) lr.patient_uuid as personuuid12, 
lr.ods_datim_id crypt_ods_datim_id,CAST(lr.date_result_reported AS DATE) AS dateOfLastCrytococalAntigen, 
lr.result_reported AS lastCrytococalAntigen, 
ROW_NUMBER() OVER (PARTITION BY lr.patient_uuid ORDER BY lr.date_result_reported DESC) as rowNum 
from public.%I lt 
inner join public.%I lr on lr.test_id = lt.id 
where (lab_test_id = 52 OR lab_test_id = 69 OR lab_test_id = 70)
AND lr.date_result_reported IS NOT NULL 
AND lr.date_result_reported <= %L
AND lr.date_result_reported >= ''1980-01-01''
AND lr.result_reported is NOT NULL 
AND lr.archived = 0 
) dt where rowNum = 1',
ctecrytococalantigen_partition,laboratorytest_partition,
laboratoryresult_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctecrytococalantigen_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_crytococal_antigen(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_crytococal_antigen('tZy8wIM53xT');