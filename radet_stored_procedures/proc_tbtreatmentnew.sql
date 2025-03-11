-- PROCEDURE: expanded_radet_client.proc_tbtreatmentnew(character varying)

DROP PROCEDURE IF EXISTS expanded_radet_client.proc_tbtreatmentnew_v2(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_tbtreatmentnew(
IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
start_time TIMESTAMP;
end_time TIMESTAMP;
ctetbTreatmentNew_partition TEXT;
hivobservation_partition TEXT;
period_end_date DATE;
radetmonitoringpartition text;

BEGIN
-- Get the active period's end date
SELECT date 
INTO period_end_date
FROM expanded_radet.period 
WHERE is_active;

-- Generate partition table names dynamically
ctetbTreatmentNew_partition := CONCAT('cte_tbtreatmentnew_', datim_id);
hivobservation_partition := CONCAT('hiv_observation_', datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

-- Log the start time
SELECT TIMEOFDAY() INTO start_time;

-- Delete existing data for the given `datim_id`
DELETE FROM expanded_radet_client.cte_tbTreatmentNew
WHERE ods_datim_id_tbNew = datim_id;

RAISE NOTICE 'Successfully truncated % table', ctetbTreatmentNew_partition;

-- Insert transformed data into the target table
EXECUTE FORMAT(
'INSERT INTO expanded_radet_client.cte_tbTreatmentNew
WITH tb_start AS (
SELECT * FROM(
SELECT person_uuid AS person_uuid,ods_datim_id,date_of_observation AS screeningDate,
CASE WHEN data->''tptMonitoring''->>''tbTreatmentStartDate'' ~ ''^\d{4}-\d{2}-\d{2}$'' 
THEN CAST(data->''tptMonitoring''->>''tbTreatmentStartDate'' AS DATE) ELSE NULL 
END AS tbTreatmentStartDate,data->''tbIptScreening''->>''tbTestResult'' AS tbDiagnosticResult,
data->''tbIptScreening''->>''chestXrayResult'' AS chestXrayResult,
data->''tbIptScreening''->>''diagnosticTestType'' AS tbDiagnosticTestType,
COALESCE(NULLIF(data->''tptMonitoring''->>''tbType'', ''''), NULLIF(data->''tbIptScreening''->>''tbType'', '''')) AS tbTreatmentType,
CASE WHEN data->''tbIptScreening''->>''dateSpecimenSent'' ~ ''^\d{4}-\d{2}-\d{2}$'' 
THEN CAST(data->''tbIptScreening''->>''dateSpecimenSent'' AS DATE) ELSE NULL END AS specimenSentDate,
data->''tbIptScreening''->>''status'' AS screeningStatus,
CASE WHEN data->''tbIptScreening''->>''dateOfDiagnosticTest'' ~ ''^\d{4}-\d{2}-\d{2}$'' 
THEN CAST(data->''tbIptScreening''->>''dateOfDiagnosticTest'' AS DATE)
ELSE NULL END AS dateOfDiagnosticTest,
data->''tbIptScreening''->>''tbScreeningType'' AS tbScreeningType,
ROW_NUMBER() OVER (PARTITION BY person_uuid ORDER BY date_of_observation DESC) AS rnk3
FROM public.%I
WHERE archived = 0 
AND ((data->''tbIptScreening''->>''status'' LIKE ''%%Presumptive TB'' 
OR data->''tbIptScreening''->>''status'' = ''No signs or symptoms of TB'')
AND (data->''tbIptScreening''->>''outcome'' = ''Presumptive TB'' 
OR data->''tbIptScreening''->>''outcome'' = ''Not Presumptive'')) ) U
WHERE rnk3 = 1) ,

tb_completion AS (
SELECT 
person_uuid AS person_uuid,
ods_datim_id,
CASE 
WHEN data->''tbIptScreening''->>''completionDate'' ~ ''^\d{4}-\d{2}-\d{2}$'' 
THEN CAST(data->''tbIptScreening''->>''completionDate'' AS DATE) ELSE NULL 
END AS completionDate,
data->''tbIptScreening''->>''treatmentOutcome'' AS treatmentOutcome
FROM public.%I
WHERE 
data->''tbIptScreening''->>''completionDate'' IS NOT NULL 
AND data->''tbIptScreening''->>''completionDate'' != ''''
AND data->''tbIptScreening''->>''treatmentOutcome'' IS NOT NULL 
AND data->''tbIptScreening''->>''treatmentOutcome'' != ''''
AND archived = 0
)
SELECT
COALESCE(ts.person_uuid, tc.person_uuid) AS person_uuid_tbNew,
COALESCE(ts.ods_datim_id, tc.ods_datim_id) AS ods_datim_id_tbNew,
ts.tbTreatmentStartDate,
COALESCE(ts.tbDiagnosticResult, ts.chestXrayResult) AS tbDiagnosticResult,
ts.tbDiagnosticTestType,
ts.tbScreeningType,
ts.screeningStatus,
ts.tbTreatmentType,
ts.screeningDate,
ts.specimenSentDate,
ts.dateOfDiagnosticTest,
tc.completionDate,
tc.treatmentOutcome
FROM tb_start ts
FULL OUTER JOIN tb_completion tc ON ts.person_uuid = tc.person_uuid
ORDER BY screeningDate DESC;',
hivobservation_partition, hivobservation_partition
);

-- Log the end time
SELECT TIMEOFDAY() INTO end_time;

-- Insert monitoring information
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctetbTreatmentNew_partition,start_time,end_time,datim_id);

END;
$BODY$;

-- Set the owner of the procedure
ALTER PROCEDURE expanded_radet_client.proc_tbtreatmentnew(character varying)
OWNER TO lamisplus_etl;


CALL expanded_radet_client.proc_tbtreatmentnew('tZy8wIM53xT');
-- SELECT COUNT(1) FROM expanded_radet_client."cte_tbtreatmentnew_tZy8wIM53xT"