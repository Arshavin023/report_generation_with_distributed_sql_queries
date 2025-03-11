-- PROCEDURE: expanded_radet_client.proc_ovc(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_ovc(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_ovc(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE cteovc_partition text;
DECLARE hivenrollment_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
cteovc_partition := CONCAT('cte_ovc_',datim_id);
hivenrollment_partition := CONCAT('hiv_enrollment_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',cteovc_partition);

RAISE NOTICE 'successfully truncate % table', cteovc_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT DISTINCT ON (person_uuid) person_uuid AS personUuid100,
ods_datim_id ovc_ods_datim_id,ovc_number AS ovcNumber,
house_hold_number AS householdNumber
FROM public.%I ',cteovc_partition,hivenrollment_partition);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,cteovc_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_ovc(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_ovc('tZy8wIM53xT');