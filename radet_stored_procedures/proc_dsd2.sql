-- PROCEDURE: expanded_radet_client.proc_dsd2(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_dsd2(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_dsd2(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctedsd2_partition text;
DECLARE dsddevolvement_partition text;
DECLARE baseappcodeset_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctedsd2_partition := CONCAT('cte_dsd2_',datim_id);
dsddevolvement_partition := CONCAT('dsd_devolvement_',datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctedsd2_partition);

RAISE NOTICE 'successfully truncate % table', ctedsd2_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
select d2.person_uuid as person_uuid_dsd_2, d2.ods_datim_id dsd2_ods_datim_id,
d2.dateOfCurrentDSD, d2.currentDSDModel, d2.dateReturnToSite, bac.display as currentDsdOutlet 
from (select d.person_uuid, d.ods_datim_id,
d.date_devolved as dateOfCurrentDSD, bmt.display as currentDSDModel, d.date_return_to_site AS dateReturnToSite, outlet_name as dsdOutlet, 
ROW_NUMBER() OVER (PARTITION BY d.person_uuid ORDER BY d.date_devolved DESC ) AS row 
from public.%I d 
left join public.%I bmt on bmt.code = d.dsd_type 
where d.archived = 0 
and d.date_devolved between ''1980-01-01'' and %L
) d2 
left join public.%I bac on bac.code = d2.dsdOutlet where d2.row = 1
',ctedsd2_partition,dsddevolvement_partition,baseappcodeset_partition,
period_end_date,baseappcodeset_partition);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctedsd2_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_dsd2(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_dsd2('tZy8wIM53xT');