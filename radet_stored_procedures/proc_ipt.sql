-- PROCEDURE: expanded_radet_client.proc_ipt(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_ipt(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_ipt(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE cteipt_partition text;
DECLARE hivartpharmacy_partition text;
DECLARE hivobservation_partition text;
DECLARE laboratorytest_partition text;
DECLARE hivregimen_partition text;
DECLARE hivregimentype_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
cteipt_partition := CONCAT('cte_ipt_',datim_id);
hivartpharmacy_partition := CONCAT('hiv_art_pharmacy_',datim_id);
hivobservation_partition := CONCAT('hiv_observation_',datim_id);
hivregimen_partition := CONCAT('hiv_regimen_',datim_id);
hivregimentype_partition := CONCAT('hiv_regimen_type_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',cteipt_partition);

RAISE NOTICE 'successfully truncate % table', cteipt_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
with ipt_c as ( 
select person_uuid,ods_datim_id, date_completed as iptCompletionDate, iptCompletionStatus 
from ( 
select person_uuid, ods_datim_id,cast(ipt->>''dateCompleted'' as date) as date_completed, 
COALESCE(NULLIF(CAST(ipt->>''completionStatus'' AS text), ''''), '''') AS iptCompletionStatus, 
row_number () over (partition by person_uuid order by cast(ipt->>''dateCompleted'' as date) desc) as rnk 
from public.%I 
where (ipt->>''dateCompleted'' is not null
and ipt->>''dateCompleted'' != ''null'' and ipt->>''dateCompleted'' != '''' 
AND TRIM(ipt->>''dateCompleted'') <> '''') 
and archived = 0) ic where ic.rnk = 1
),

ipt_s as ( 
SELECT person_uuid, visit_date as dateOfIptStart, regimen_name as iptType 
FROM ( 
SELECT h.person_uuid, h.visit_date, CAST(pharmacy_object ->> ''regimenName'' AS VARCHAR) AS regimen_name, 
ROW_NUMBER() OVER (PARTITION BY h.person_uuid ORDER BY h.visit_date ASC) AS rnk 
FROM public.%I h 
INNER JOIN jsonb_array_elements(h.extra -> ''regimens'') WITH ORDINALITY p(pharmacy_object) ON TRUE 
INNER JOIN public.%I hr ON hr.description = CAST(p.pharmacy_object ->> ''regimenName'' AS VARCHAR) 
INNER JOIN public.%I hrt ON hrt.id = hr.regimen_type_id AND hrt.id = 15 
-- AND hrt.id NOT IN (1,2,3,4,14, 16) 
WHERE 
-- hrt.id = 15 AND
h.archived = 0 
) AS ic 
WHERE ic.rnk = 1 ), 

ipt_c_cs as ( 
SELECT person_uuid, ods_datim_id,iptStartDate, iptCompletionSCS, iptCompletionDSC 
FROM ( 
SELECT person_uuid, ods_datim_id, CASE
WHEN (data->''tbIptScreening''->>''dateTPTStart'') IS NULL 
OR (data->''tbIptScreening''->>''dateTPTStart'') = '''' 
OR (data->''tbIptScreening''->>''dateTPTStart'') = '' ''  THEN NULL
ELSE CAST((data->''tbIptScreening''->>''dateTPTStart'') AS DATE)
END as iptStartDate, 
data->''tptMonitoring''->>''outComeOfIpt'' as iptCompletionSCS, 
CASE 
WHEN (data->''tptMonitoring''->>''date'') = ''null'' OR (data->''tptMonitoring''->>''date'') = '''' OR (data->''tptMonitoring''->>''date'') = '' ''  THEN NULL 
ELSE cast(data->''tptMonitoring''->>''date'' as date) 
END as iptCompletionDSC, 
ROW_NUMBER() OVER (PARTITION BY person_uuid ORDER BY 
CASE  WHEN (data->''tptMonitoring''->>''date'') = ''null'' OR (data->''tptMonitoring''->>''date'') = '''' OR (data->''tptMonitoring''->>''date'') = '' ''  THEN NULL 
ELSE cast(data->''tptMonitoring''->>''date'' as date) 
END  DESC) AS ipt_c_sc_rnk 
FROM public.%I  
WHERE type = ''Chronic Care'' 
AND archived = 0 
AND (data->''tptMonitoring''->>''date'') IS NOT NULL 
AND (data->''tptMonitoring''->>''date'') != ''null'' 
) AS ipt_ccs 
WHERE ipt_c_sc_rnk = 1
) 
select COALESCE(ipt_c.person_uuid,ipt_c_cs.person_uuid) as personuuid80,
COALESCE(ipt_c.ods_datim_id,ipt_c_cs.ods_datim_id) as cte_ipt_ods_datim_id,
CASE WHEN coalesce(ipt_c_cs.iptCompletionDSC, ipt_c.iptCompletionDate) > %L
THEN NULL ELSE coalesce(ipt_c_cs.iptCompletionDSC, ipt_c.iptCompletionDate) 
END as iptCompletionDate, 
coalesce(ipt_c_cs.iptCompletionSCS, ipt_c.iptCompletionStatus) as iptCompletionStatus, 
COALESCE(ipt_s.dateOfIptStart, ipt_c_cs.iptStartDate) AS dateOfIptStart, ipt_s.iptType 
from ipt_c 
left join ipt_s on ipt_s.person_uuid = ipt_c.person_uuid 
left join ipt_c_cs on ipt_s.person_uuid = ipt_c_cs.person_uuid
',cteipt_partition,hivartpharmacy_partition,hivartpharmacy_partition,
hivregimen_partition,hivregimentype_partition,
hivobservation_partition,period_end_date);

SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,cteipt_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_ipt(character varying)
    OWNER TO lamisplus_etl;
-- CALL expanded_radet_client.proc_ipt('tZy8wIM53xT');