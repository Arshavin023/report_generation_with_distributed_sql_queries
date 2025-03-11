-- PROCEDURE: expanded_radet_client.proc_current_regimen(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_current_regimen(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_current_regimen(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctecurrentregimen_partition text;
DECLARE hivartpharmacy_partition text;
DECLARE hivartpharmacyregimens_partition text;
DECLARE hivregimen_partition text;
DECLARE hivregimentype_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctecurrentregimen_partition := CONCAT('cte_current_regimen_',datim_id);
hivartpharmacy_partition := CONCAT('hiv_art_pharmacy_',datim_id);
-- revert back to this later
hivartpharmacyregimens_partition := CONCAT('hiv_art_pharmacy_regimens_',datim_id);
-- hivartpharmacyregimens_partition := CONCAT('hapr_20240930_',datim_id);
hivregimen_partition := CONCAT('hiv_regimen_',datim_id);
hivregimentype_partition := CONCAT('hiv_regimen_type_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctecurrentregimen_partition);

RAISE NOTICE 'successfully truncate % table', ctecurrentregimen_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT DISTINCT ON (regiment_table.person_uuid) regiment_table.person_uuid AS person_uuid70,
regiment_table.ods_datim_id creg_ods_datim_id,
start_of_regimen AS DateOfStartOfCurrentARTRegimen,
regiment_table.max_visit_date,
regiment_table.regimen
FROM (SELECT MIN(visit_date) start_of_regimen,
MAX(visit_date) max_visit_date,regimen,
person_uuid,ods_datim_id
FROM (SELECT hap.id,hap.person_uuid,hap.ods_datim_id,hap.visit_date,
hivreg.description AS regimen,
ROW_NUMBER() OVER(ORDER BY person_uuid,visit_date) rn1,
ROW_NUMBER() OVER(PARTITION BY hivreg.description ORDER BY person_uuid,visit_date) rn2
FROM public.%I AS hap
INNER JOIN (SELECT MAX(hapr.id) AS id,art_pharmacy_id,regimens_id,hr.description
FROM public.%I AS hapr
INNER JOIN public.%I AS hr ON hapr.regimens_id = hr.id
WHERE hr.regimen_type_id IN (1,2,3,4,14, 16)
GROUP BY art_pharmacy_id, regimens_id, hr.description
) AS hapr ON hap.id = hapr.art_pharmacy_id and hap.archived=0
INNER JOIN public.%I AS hivreg ON hapr.regimens_id = hivreg.id
INNER JOIN public.%I AS hivregtype ON hivreg.regimen_type_id = hivregtype.id
AND hivreg.regimen_type_id IN (1,2,3,4,14,16)
ORDER BY person_uuid, visit_date) t
GROUP BY person_uuid,ods_datim_id,regimen,rn1 - rn2
ORDER BY MIN(visit_date)) AS regiment_table
INNER JOIN (SELECT DISTINCT MAX(visit_date) AS max_visit_date,person_uuid
FROM public.%I WHERE archived=0
GROUP BY person_uuid
) AS hap ON regiment_table.person_uuid = hap.person_uuid
WHERE regiment_table.max_visit_date = hap.max_visit_date
GROUP BY regiment_table.person_uuid,regiment_table.ods_datim_id,regiment_table.regimen,
regiment_table.max_visit_date,start_of_regimen',
ctecurrentregimen_partition,
hivartpharmacy_partition,hivartpharmacyregimens_partition,
hivregimen_partition,hivregimen_partition,hivregimentype_partition,
hivartpharmacy_partition);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctecurrentregimen_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_current_regimen(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_current_regimen('tZy8wIM53xT');