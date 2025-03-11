-- PROCEDURE: expanded_radet_client.proc_vacauseofdeath(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_vacauseofdeath(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_vacauseofdeath(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctevacauseofdeath_partition text;
DECLARE hivstatustracker_partition text;
DECLARE hivenrollment_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctevacauseofdeath_partition := CONCAT('cte_vacauseofdeath_',datim_id);
hivstatustracker_partition := CONCAT('hiv_status_tracker_',datim_id);
hivenrollment_partition := CONCAT('hiv_enrollment_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctevacauseofdeath_partition);

RAISE NOTICE 'successfully truncate % table', ctevacauseofdeath_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT hst.person_id,  hst.ods_datim_id, hst.hiv_status, 
hst.cause_of_death, hst.va_cause_of_death,hst.status_date
FROM (SELECT * FROM (SELECT DISTINCT (person_id) person_id, ods_datim_id,status_date, 
cause_of_death, va_cause_of_death,
hiv_status, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY status_date DESC)
FROM public.%I 
WHERE hiv_status ilike ''%%Died%%'' AND archived=0 
AND status_date <= %L
)s
WHERE s.row_number=1) hst
INNER JOIN public.%I he ON he.person_uuid = hst.person_id
',ctevacauseofdeath_partition,hivstatustracker_partition,
period_end_date,hivenrollment_partition);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctevacauseofdeath_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_vacauseofdeath(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_vacauseofdeath('tZy8wIM53xT');