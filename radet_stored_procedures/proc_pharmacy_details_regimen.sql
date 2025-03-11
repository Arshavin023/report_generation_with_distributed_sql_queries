-- PROCEDURE: expanded_radet_client.proc_pharmacy_details_regimen(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_pharmacy_details_regimen(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_pharmacy_details_regimen(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctepharmacydetailsregimen_partition text;
DECLARE hivartpharmacy_partition text;
DECLARE hivartpharmacyregimens_partition text;
DECLARE hivregimen_partition text;
DECLARE hivregimentype_partition text;
DECLARE baseappcodeset_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctepharmacydetailsregimen_partition := CONCAT('cte_pharmacy_details_regimen_',datim_id);
hivartpharmacy_partition := CONCAT('hiv_art_pharmacy_',datim_id);
-- revert back to this later
hivartpharmacyregimens_partition := CONCAT('hiv_art_pharmacy_regimens_',datim_id);
-- hivartpharmacyregimens_partition := CONCAT('hapr_20240930_',datim_id);
hivregimen_partition := CONCAT('hiv_regimen_',datim_id);
hivregimentype_partition := CONCAT('hiv_regimen_type_',datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctepharmacydetailsregimen_partition);

RAISE NOTICE 'successfully truncate % table', ctepharmacydetailsregimen_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
select * from (
select *, ROW_NUMBER() OVER (PARTITION BY pr1.person_uuid40 ORDER BY pr1.lastpickupdate DESC) as rnk3
from (SELECT p.person_uuid as person_uuid40, p.ods_datim_id pharma_ods_datim_id,
COALESCE(ds_model.display, p.dsd_model_type) as dsdModel, p.visit_date as lastpickupdate,
r.description as currentARTRegimen, rt.description as currentRegimenLine,
p.next_appointment as nextPickupDate,
CAST(p.refill_period /30.0 AS DECIMAL(10,1)) AS monthsOfARVRefill
from public.%I p
INNER JOIN public.%I pr ON pr.art_pharmacy_id = p.id
INNER JOIN public.%I r on r.id = pr.regimens_id
INNER JOIN public.%I rt on rt.id = r.regimen_type_id
left JOIN public.%I ds_model on ds_model.code = p.dsd_model_type 
WHERE r.regimen_type_id in (1,2,3,4,14, 16)
AND  p.archived = 0
AND  p.visit_date >= ''1980-01-01''
AND  p.visit_date  < %L) as pr1
) as pr2
where pr2.rnk3 = 1',
ctepharmacydetailsregimen_partition,
hivartpharmacy_partition,hivartpharmacyregimens_partition,
hivregimen_partition,hivregimentype_partition,
baseappcodeset_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctepharmacydetailsregimen_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_pharmacy_details_regimen(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_pharmacy_details_regimen('tZy8wIM53xT');