-- PROCEDURE: expanded_radet_client.proc_ipt_s(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_ipt_s(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_ipt_s(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE cteipts_partition text;
DECLARE hivartpharmacy_partition text;
DECLARE hivregimen_partition text;
DECLARE hivregimentype_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
cteipts_partition := CONCAT('cte_ipt_s_',datim_id);
hivartpharmacy_partition := CONCAT('hiv_art_pharmacy_',datim_id);
hivregimen_partition := CONCAT('hiv_regimen_',datim_id);
hivregimentype_partition := CONCAT('hiv_regimen_type_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',cteipts_partition);

RAISE NOTICE 'successfully truncate % table', cteipts_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT person_uuid person_uuid_ipt_s, ods_datim_id ods_datim_id_ipt_s,
visit_date as dateOfIptStart, regimen_name as iptType 
FROM ( 
SELECT h.person_uuid,h.ods_datim_id,
h.visit_date, CAST(pharmacy_object ->> ''regimenName'' AS VARCHAR) AS regimen_name, 
ROW_NUMBER() OVER (PARTITION BY h.person_uuid ORDER BY h.visit_date ASC) AS rnk 
FROM public.%I h 
INNER JOIN jsonb_array_elements(h.extra -> ''regimens'') WITH ORDINALITY p(pharmacy_object) ON TRUE 
INNER JOIN public.%I hr ON hr.description = CAST(p.pharmacy_object ->> ''regimenName'' AS VARCHAR) 
INNER JOIN public.%I hrt ON hrt.id = hr.regimen_type_id  
AND hrt.id = 15 
--AND hrt.id NOT IN (1,2,3,4,14, 16) 
WHERE h.archived = 0 
--hrt.id = 15 AND 
) AS ic 
WHERE ic.rnk = 1',cteipts_partition,hivartpharmacy_partition,
hivregimen_partition,hivregimentype_partition);

SELECT TIMEOFDAY() INTO end_time;
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,cteipts_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_ipt_s(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_ipt_s('tZy8wIM53xT');