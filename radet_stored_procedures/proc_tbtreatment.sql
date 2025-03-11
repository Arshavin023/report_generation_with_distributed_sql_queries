-- PROCEDURE: expanded_radet_client.proc_tbtreatment(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_tbtreatment(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_tbtreatment(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctetbTreatment_partition text;
DECLARE hivobservation_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctetbTreatment_partition := CONCAT('cte_tbtreatment_',datim_id);
hivobservation_partition := CONCAT('hiv_observation_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctetbTreatment_partition);

RAISE NOTICE 'successfully truncate % table', ctetbTreatment_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT * FROM (SELECT
person_uuid as tbTreatmentPersonUuid,ods_datim_id tbtreat_ods_datim_id,
COALESCE(NULLIF(CAST(data->''tbIptScreening''->>''treatementType'' AS text), ''''), '''') as tbTreatementType,
NULLIF(CAST(NULLIF(data->''tbIptScreening''->>''tbTreatmentStartDate'', '''') AS DATE), NULL)as tbTreatmentStartDate,
CAST(data->''tbIptScreening''->>''treatmentOutcome'' AS text) as tbTreatmentOutcome,
NULLIF(CAST(NULLIF(data->''tbIptScreening''->>''completionDate'', '''') AS DATE), NULL) as tbCompletionDate,
ROW_NUMBER() OVER ( PARTITION BY person_uuid ORDER BY date_of_observation DESC)
FROM public.%I
WHERE type = ''Chronic Care'' and archived = 0) tbTreatment 
WHERE row_number = 1 AND tbTreatmentStartDate IS NOT NULL',
ctetbTreatment_partition,hivobservation_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctetbTreatment_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_tbtreatment(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_tbtreatment('tZy8wIM53xT');