-- PROCEDURE: expanded_radet_client.proc_eac(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_eac(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_eac(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE cteeac_partition text;
DECLARE hiveac_partition text;
DECLARE hiveacsession_partition text;
DECLARE laboratoryresult_partition text;
DECLARE laboratorytest_partition text;
DECLARE laboratorysample_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
cteeac_partition := CONCAT('cte_eac_',datim_id);
hiveac_partition := CONCAT('hiv_eac_',datim_id);
hiveacsession_partition := CONCAT('hiv_eac_session_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
laboratorysample_partition := CONCAT('laboratory_sample_',datim_id);
laboratoryresult_partition := CONCAT('laboratory_result_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',cteeac_partition);

RAISE NOTICE 'successfully truncate % table', cteeac_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
with current_eac as (SELECT * FROM (
select id, person_uuid, ods_datim_id,uuid, status, 
ROW_NUMBER() OVER (PARTITION BY person_uuid ORDER BY id DESC) AS row 
from public.%I where archived = 0) U 
WHERE row=1
),

first_eac as ( 
select * from 
(select ce.id, ce.person_uuid,ce.ods_datim_id, hes.eac_session_date, 
ROW_NUMBER() OVER (PARTITION BY hes.person_uuid ORDER BY hes.eac_session_date ASC) AS row 
from public.%I hes 
join current_eac ce on ce.uuid = hes.eac_id AND ce.person_uuid=hes.person_uuid
where hes.archived = 0 
and hes.eac_session_date between ''1980-01-01'' and %L
and hes.status in (''FIRST EAC'')
) as fes where row = 1 
), 

last_eac as ( 
select * from (
select ce.id, ce.person_uuid, hes.eac_session_date, 
ROW_NUMBER() OVER (PARTITION BY hes.person_uuid ORDER BY hes.eac_session_date DESC ) AS row
from public.%I hes 
join current_eac ce on ce.uuid = hes.eac_id AND ce.person_uuid=hes.person_uuid
where hes.archived = 0 
and hes.eac_session_date between ''1980-01-01'' and %L
and hes.status in (''FIRST EAC'', ''SECOND EAC'', ''THIRD EAC'')
) as les where row = 1 
), 

eac_count as (
select person_uuid, CASE WHEN count(*) > 6 THEN 6 ELSE count(*) END as no_eac_session 
from (
select hes.person_uuid 
from public.%I hes 
join current_eac ce on ce.person_uuid = hes.person_uuid 
where hes.archived = 0 
and hes.eac_session_date between ''1980-01-01'' and %L 
and hes.status in (''FIRST EAC'', ''SECOND EAC'', ''THIRD EAC'') 
) as c group by person_uuid 
), 

extended_eac as (
select * from ( 
select ce.id, ce.person_uuid, hes.eac_session_date, 
ROW_NUMBER() OVER (PARTITION BY hes.person_uuid ORDER BY hes.eac_session_date DESC ) AS row 
from public.%I hes 
join current_eac ce on ce.uuid = hes.eac_id 
where hes.archived = 0 and hes.status is not null 
and hes.eac_session_date between ''1980-01-01'' and %L
and hes.status not in (''FIRST EAC'', ''SECOND EAC'', ''THIRD EAC'')
) as exe where row = 1 
), 

post_eac_vl as ( 
select * from (select lt.patient_uuid, cast(ls.date_sample_collected as date), 
lr.result_reported, cast(lr.date_result_reported as date), 
ROW_NUMBER() OVER (PARTITION BY lt.patient_uuid ORDER BY ls.date_sample_collected DESC) AS row 
from public.%I lt 
left join public.%I ls on ls.test_id = lt.id 
left join public.%I lr on lr.test_id = lt.id 
where lt.viral_load_indication = 302 and lt.archived = 0 and ls.archived = 0 
and ls.date_sample_collected between ''1980-01-01'' and %L
) pe where row = 1 
)

select fe.person_uuid as person_uuid50,%L eac_ods_datim_id,
fe.eac_session_date as dateOfCommencementOfEAC, le.eac_session_date as dateOfLastEACSessionCompleted, 
ec.no_eac_session as numberOfEACSessionCompleted, exe.eac_session_date as dateOfExtendEACCompletion, 
pvl.result_reported as repeatViralLoadResult, pvl.date_result_reported as DateOfRepeatViralLoadResult, 
pvl.date_sample_collected as dateOfRepeatViralLoadEACSampleCollection 
from first_eac fe 
left join last_eac le on le.person_uuid = fe.person_uuid 
left join eac_count ec on ec.person_uuid = fe.person_uuid 
left join extended_eac exe on exe.person_uuid = fe.person_uuid 
left join post_eac_vl pvl on pvl.patient_uuid = fe.person_uuid
',cteeac_partition, hiveac_partition,hiveacsession_partition,period_end_date,
hiveacsession_partition,period_end_date,
hiveacsession_partition,period_end_date,
hiveacsession_partition,period_end_date,
laboratorytest_partition,laboratorysample_partition,laboratoryresult_partition,
period_end_date,datim_id);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,cteeac_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_eac(character varying)
    OWNER TO lamisplus_etl;

-- CALL expanded_radet_client.proc_eac('tZy8wIM53xT');