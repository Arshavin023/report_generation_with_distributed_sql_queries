-- PROCEDURE: ahd.proc_current_status(character varying)

-- DROP PROCEDURE IF EXISTS ahd.proc_current_status(character varying);

CREATE OR REPLACE PROCEDURE ahd.proc_current_status(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctecurrentstatus_partition text;
DECLARE hivartpharmacy_partition text;
DECLARE hivartpharmacyregimens_partition text;
DECLARE hivenrollment_partition text;
DECLARE hivregimen_partition text;
DECLARE hivregimentype_partition text;
DECLARE hivstatustracker_partition text;
DECLARE period_end_date DATE;
DECLARE ahdmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM ahd.period WHERE is_active; -----?3

ctecurrentstatus_partition := CONCAT('cte_current_status_',datim_id);
hivartpharmacy_partition := CONCAT('hiv_art_pharmacy_',datim_id);
hivenrollment_partition := CONCAT('hiv_enrollment_',datim_id);
hivartpharmacyregimens_partition := CONCAT('hiv_art_pharmacy_regimens_',datim_id);
hivregimen_partition := CONCAT('hiv_regimen_',datim_id);
hivregimentype_partition := CONCAT('hiv_regimen_type_',datim_id);
hivstatustracker_partition := CONCAT('hiv_status_tracker_',datim_id);
ahdmonitoringpartition := CONCAT('ahd_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

-- EXECUTE FORMAT('TRUNCATE ahd.%I',ctecurrentstatus_partition);

DELETE FROM ahd.cte_current_status
WHERE cus_ods_datim_id=datim_id;

RAISE NOTICE 'successfully truncate % table', ctecurrentstatus_partition;

EXECUTE FORMAT('INSERT INTO ahd.%I
SELECT DISTINCT ON (pharmacy.person_uuid) pharmacy.person_uuid AS cupersonuuid,
%L cus_ods_datim_id,
(CASE WHEN stat.hiv_status ILIKE ''%%DEATH%%'' OR stat.hiv_status ILIKE ''%%Died%%'' THEN ''Died''
WHEN(stat.status_date > pharmacy.maxdate
AND (stat.hiv_status ILIKE ''%%stop%%'' OR stat.hiv_status ILIKE ''%%out%%'' 
OR stat.hiv_status ILIKE ''%%Invalid %%'' OR stat.hiv_status ILIKE ''%%ART Transfer In%%'')
) THEN stat.hiv_status ELSE pharmacy.status
END) AS status,

(CASE WHEN stat.hiv_status ILIKE ''%%DEATH%%'' OR stat.hiv_status ILIKE ''%%Died%%''  THEN stat.status_date
WHEN(stat.status_date > pharmacy.maxdate
AND (stat.hiv_status ILIKE ''%%stop%%'' OR stat.hiv_status ILIKE ''%%out%%'' 
OR stat.hiv_status ILIKE ''%%Invalid %%'' OR stat.hiv_status ILIKE ''%%ART Transfer In%%'')
) THEN stat.status_date ELSE pharmacy.visit_date END) AS status_date,
pharmacy.refill_period,stat.cause_of_death, stat.va_cause_of_death

FROM
(SELECT (CASE WHEN hp.visit_date + hp.refill_period + INTERVAL ''29 day'' <= %L THEN ''IIT''
ELSE ''Active'' END) status,hp.refill_period,
(CASE WHEN hp.visit_date + hp.refill_period + INTERVAL ''29 day'' <= %L  THEN hp.visit_date + hp.refill_period + INTERVAL ''29 day''
ELSE hp.visit_date END) AS visit_date,hp.person_uuid, hp.ods_datim_id,MAXDATE
FROM public.%I hp
INNER JOIN (SELECT hap.id, hap.person_uuid, hap.visit_date AS  MAXDATE, 
hap.ods_datim_id AS hap_ods_datim_id,
ROW_NUMBER() OVER (PARTITION BY hap.person_uuid ORDER BY hap.visit_date DESC) as rnkkk3
FROM public.%I hap 
INNER JOIN public.%I pr ON pr.art_pharmacy_id = hap.id 
INNER JOIN public.%I h ON h.person_uuid = hap.person_uuid AND h.archived = 0 
INNER JOIN public.%I r on r.id = pr.regimens_id 
INNER JOIN public.%I rt on rt.id = r.regimen_type_id 
WHERE r.regimen_type_id in (1,2,3,4,14, 16) 
AND hap.archived = 0                
AND hap.visit_date <= %L
) MAX ON MAX.MAXDATE = hp.visit_date 
AND MAX.person_uuid = hp.person_uuid 
AND MAX.id=hp.id AND MAX.hap_ods_datim_id=hp.ods_datim_id
AND MAX.rnkkk3 = 1 
WHERE hp.archived = 0 AND hp.visit_date <= %L
) pharmacy

LEFT JOIN (
SELECT hst.hiv_status,hst.person_id,hst.ods_datim_id,hst.cause_of_death,hst.va_cause_of_death,hst.status_date
FROM (SELECT * FROM (SELECT person_id, ods_datim_id,status_date, cause_of_death,va_cause_of_death,
hiv_status, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY status_date DESC)
FROM public.%I WHERE archived=0 AND status_date <= %L)s
WHERE s.row_number=1) hst
INNER JOIN public.%I he ON he.person_uuid = hst.person_id AND he.ods_datim_id=hst.ods_datim_id
WHERE hst.status_date <= %L) stat ON stat.person_id = pharmacy.person_uuid
AND stat.ods_datim_id = pharmacy.ods_datim_id',
ctecurrentstatus_partition,datim_id,period_end_date,period_end_date,
hivartpharmacy_partition,hivartpharmacy_partition, hivartpharmacyregimens_partition,
hivenrollment_partition,hivregimen_partition,hivregimentype_partition,
period_end_date,period_end_date,hivstatustracker_partition,
period_end_date,hivenrollment_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO ahd.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
ahdmonitoringpartition,ctecurrentstatus_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE ahd.proc_current_status(character varying)
    OWNER TO lamisplus_etl;

-- CALL ahd.proc_current_status('A1xxdELs2fm');
-- SELECT * FROM ahd.ahd_monitoring;