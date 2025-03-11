-- PROCEDURE: expanded_radet_client.proc_iptnew(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_iptnew(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_iptnew(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE cteiptnew_partition text;
DECLARE hivobservation_partition text;
DECLARE laboratorytest_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
cteiptnew_partition := CONCAT('cte_iptnew_',datim_id);
hivobservation_partition := CONCAT('hiv_observation_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',cteiptnew_partition);

RAISE NOTICE 'successfully truncate % table', cteiptnew_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
WITH tpt_completed AS (
SELECT * FROM (
SELECT person_uuid AS person_uuid,ods_datim_id,
data->''tptMonitoring''->>''endedTpt'' AS endedTpt,
NULLIF(CAST(NULLIF(data->''tptMonitoring''->>''dateTptEnded'', '''') AS DATE), NULL) AS tptCompletionDate,
data->''tptMonitoring''->>''outComeOfIpt'' AS tptCompletionStatus,
data->''tbIptScreening''->>''outcome'' AS completion_tptPreventionOutcome, 
ROW_NUMBER () OVER (PARTITION BY person_uuid ORDER BY date_of_observation  DESC) rowNum
FROM public.%I 
WHERE data->''tptMonitoring''->>''endedTpt'' = ''Yes'' AND 
data->''tptMonitoring''->>''dateTptEnded'' IS NOT NULL AND
data->''tptMonitoring''->>''dateTptEnded'' != ''''
AND archived = 0) subTc WHERE rowNum = 1
),

pt_screened AS (
SELECT person_uuid AS person_uuid, ods_datim_id,
data->''tptMonitoring''->>''tptRegimen'' AS tptType,
NULLIF(CAST(NULLIF(data->''tptMonitoring''->>''dateTptStarted'', '''') AS DATE), NULL) AS tptStartDate,
data->''tptMonitoring''->>''eligibilityTpt'' AS eligibilityTpt
FROM public.%I 
WHERE (data->''tptMonitoring''->>''eligibilityTpt'' IS NOT NULL AND  data->''tptMonitoring''->>''eligibilityTpt'' != '''') 
AND (data->''tbIptScreening''->>''outcome'' IS NOT NULL AND data->''tbIptScreening''->>''outcome'' != '''' 
AND data->''tbIptScreening''->>''outcome'' != ''Currently on TPT'')
)
SELECT COALESCE(tc.person_uuid, ts.person_uuid) AS person_uuid,COALESCE(tc.ods_datim_id, ts.ods_datim_id) AS ods_datim_id,
ts.tptType,ts.tptStartDate,ts.eligibilityTpt,tc.endedTpt,tc.tptCompletionDate,tc.tptCompletionStatus
FROM pt_screened ts
FULL OUTER JOIN tpt_completed tc
ON ts.person_uuid = tc.person_uuid
',cteiptnew_partition,hivobservation_partition,
hivobservation_partition,period_end_date);

SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,cteiptnew_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_iptnew(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_iptnew('tZy8wIM53xT');