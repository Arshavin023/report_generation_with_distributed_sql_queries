-- PROCEDURE: expanded_radet_client.proc_dsd1(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_dsd1(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_dsd1(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctedsd1_partition text;
DECLARE dsddevolvement_partition text;
DECLARE baseappcodeset_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctedsd1_partition := CONCAT('cte_dsd1_',datim_id);
dsddevolvement_partition := CONCAT('dsd_devolvement_',datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctedsd1_partition);

RAISE NOTICE 'successfully truncate % table', ctedsd1_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
select person_uuid as person_uuid_dsd_1, ods_datim_id dsd1_ods_datim_id,dateOfDevolvement, modelDevolvedTo 
from (select d.person_uuid,d.ods_datim_id, d.date_devolved as dateOfDevolvement, bmt.display as modelDevolvedTo, 
ROW_NUMBER() OVER (PARTITION BY d.person_uuid ORDER BY d.date_devolved ASC ) AS row 
from public.%I d 
left join public.%I bmt on bmt.code = d.dsd_type 
where d.archived = 0 
and d.date_devolved between  ''1980-01-01'' and %L
) d1 where row = 1',ctedsd1_partition,dsddevolvement_partition,
baseappcodeset_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctedsd1_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_dsd1(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_dsd1('tZy8wIM53xT');