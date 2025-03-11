-- PROCEDURE: expanded_radet_client.proc_case_manager(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_case_manager(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_case_manager(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctecasemanager_partition text;
DECLARE casemanagerpatients_partition text; 
DECLARE casemanager_partition text; 
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active; ------?3

ctecasemanager_partition := CONCAT('cte_case_manager_',datim_id);
casemanagerpatients_partition := CONCAT('case_manager_patients_',datim_id);
casemanager_partition := CONCAT('case_manager_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctecasemanager_partition);

RAISE NOTICE 'successfully truncate % table', ctecasemanager_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT DISTINCT ON (cmp.person_uuid) person_uuid AS caseperson, 
cmp.ods_datim_id case_ods_datim_id,cmp.case_manager_id, 
CONCAT(cm.first_name, '' '', cm.last_name) AS caseManager 
FROM (SELECT person_uuid, ods_datim_id,case_manager_id,
ROW_NUMBER () OVER (PARTITION BY person_uuid ORDER BY id DESC)
FROM public.%I) cmp  
INNER JOIN public.%I cm ON cm.id=cmp.case_manager_id
WHERE cmp.row_number=1 
',ctecasemanager_partition,casemanagerpatients_partition,
casemanager_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctecasemanager_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_case_manager(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_case_manager('tZy8wIM53xT');