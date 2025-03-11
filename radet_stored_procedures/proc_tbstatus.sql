-- PROCEDURE: expanded_radet_client.proc_tbstatus(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_tbstatus(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_tbstatus(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    ctetbstatus_partition text;
    hivobservation_partition text;
    hivartclinical_partition text;
    baseappcodeset_partition text;
    period_end_date DATE;
	radetmonitoringpartition text;

BEGIN
-- Get the active period end date
SELECT date INTO period_end_date
FROM expanded_radet.period WHERE is_active;

-- Dynamic partition table names
ctetbstatus_partition := CONCAT('cte_tbstatus_', datim_id);
hivobservation_partition := CONCAT('hiv_observation_', datim_id);
hivartclinical_partition := CONCAT('hiv_art_clinical_', datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_', datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

-- Efficient row deletion
EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctetbstatus_partition);

RAISE NOTICE 'Successfully truncated table %', ctetbstatus_partition;

-- Perform the insertion
EXECUTE FORMAT('
INSERT INTO expanded_radet_client.%I
WITH FilteredObservations AS (
SELECT id, person_uuid, ods_datim_id, date_of_observation AS dateOfTbScreened,
CASE 
WHEN data->''tbIptScreening''->>''status'' = ''Presumptive TB and referred for evaluation'' THEN ''Presumptive TB''
ELSE data->''tbIptScreening''->>''status''
END AS tbStatus,
data->''tbIptScreening''->>''tbScreeningType'' AS tbScreeningType,
ROW_NUMBER() OVER (PARTITION BY person_uuid ORDER BY date_of_observation DESC) AS rowNums
FROM public.%I
WHERE type = ''Chronic Care'' 
AND data IS NOT NULL 
AND archived = 0 
AND date_of_observation BETWEEN ''1980-01-01'' AND %L
),
FilteredLatestObservations AS (
SELECT id, person_uuid, ods_datim_id, dateOfTbScreened, tbStatus, tbScreeningType
FROM FilteredObservations WHERE rowNums = 1
),

ReportingPeriod AS (
SELECT 
CASE 
WHEN EXTRACT(MONTH FROM CAST (%L AS DATE)) BETWEEN 10 AND 12 OR EXTRACT(MONTH FROM CAST (%L AS DATE)) 
BETWEEN 1 AND 3 THEN ''October - March''
WHEN EXTRACT(MONTH FROM CAST (%L AS DATE)) BETWEEN 4 AND 9 
THEN ''April - September''
END AS currentReportingPeriod
),

PresumptiveCheck AS (
SELECT lo.person_uuid, lo.ods_datim_id,
CASE 
WHEN EXISTS (
SELECT 1 FROM public.%I ho
CROSS JOIN ReportingPeriod rp
WHERE ho.person_uuid = lo.person_uuid
AND ho.type = ''Chronic Care'' 
AND ho.data IS NOT NULL 
AND (
CASE 
WHEN EXTRACT(MONTH FROM ho.date_of_observation) BETWEEN 10 AND 12 OR EXTRACT(MONTH FROM ho.date_of_observation) BETWEEN 1 AND 3 
THEN ''October - March''
WHEN EXTRACT(MONTH FROM ho.date_of_observation) BETWEEN 4 AND 9 
THEN ''April - September''
END
) = rp.currentReportingPeriod
AND ho.data->''tbIptScreening''->>''status'' ILIKE ''Presumptive TB%%''
) THEN ''Presumptive TB''
ELSE lo.tbStatus
END AS tbStatus
FROM FilteredLatestObservations lo
),
tbscreening_cs AS (
SELECT lo.id, lo.person_uuid, lo.ods_datim_id, lo.dateOfTbScreened, pc.tbStatus, lo.tbScreeningType
FROM FilteredLatestObservations lo
JOIN PresumptiveCheck pc ON lo.person_uuid = pc.person_uuid
),
tbscreening_hac AS (
SELECT h.id, h.person_uuid,h.ods_datim_id, h.visit_date, 
CAST(h.tb_screen->>''tbStatusId'' AS bigint) AS tb_status_id,
b.display AS h_status,
ROW_NUMBER() OVER (PARTITION BY h.person_uuid ORDER BY h.visit_date DESC) AS rowNums
FROM public.%I h
JOIN public.%I b ON b.id = CAST(h.tb_screen->>''tbStatusId'' AS bigint)
WHERE h.visit_date BETWEEN ''1980-01-01'' AND %L
AND h.tb_screen->>''tbStatusId'' IS NOT NULL
AND h.tb_screen->>''tbStatusId'' != ''''
)
SELECT
COALESCE(tcs.person_uuid, th.person_uuid) AS person_uuid,
COALESCE(tcs.ods_datim_id, th.ods_datim_id) AS tbstat_ods_datim_id,
COALESCE(tcs.tbStatus, th.h_status) AS tbStatus,
COALESCE(tcs.dateOfTbScreened, th.visit_date) AS dateOfTbScreened,
tcs.tbScreeningType
FROM tbscreening_cs tcs
LEFT JOIN 
(SELECT person_uuid,ods_datim_id,h_status,visit_date
FROM tbscreening_hac WHERE rowNums=1) th 
ON tcs.person_uuid = th.person_uuid
',ctetbstatus_partition, hivobservation_partition, period_end_date, 
period_end_date, period_end_date,period_end_date,
hivobservation_partition, hivartclinical_partition, 
baseappcodeset_partition, period_end_date);

-- Log procedure runtime
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctetbstatus_partition,start_time,end_time,datim_id);

END;
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_tbstatus(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_tbstatus('tZy8wIM53xT');