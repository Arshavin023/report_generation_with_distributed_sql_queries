-- PROCEDURE: expanded_radet_client.proc_current_clinical(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_current_clinical(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_current_clinical(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
start_time TIMESTAMP;
end_time TIMESTAMP;
ctecurrentclinical_partition text;
triagevitalsign_partition text;
hivartclinical_partition text;
patientperson_partition text;
hivenrollment_partition text;
baseappcodeset_partition text;
period_end_date DATE;
radetmonitoringpartition text;

BEGIN
-- Construct dynamic table names
SELECT date
INTO period_end_date
FROM expanded_radet.period WHERE is_active;

ctecurrentclinical_partition := CONCAT('cte_current_clinical_', datim_id);
triagevitalsign_partition := CONCAT('triage_vital_sign_', datim_id);
hivartclinical_partition := CONCAT('hiv_art_clinical_', datim_id);
patientperson_partition := CONCAT('patient_person_', datim_id);
hivenrollment_partition := CONCAT('hiv_enrollment_', datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_', datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

-- Log start time
SELECT TIMEOFDAY() INTO start_time;

-- Delete existing records for the specified datim_id
EXECUTE FORMAT ('TRUNCATE expanded_radet_client.%I',ctecurrentclinical_partition);

RAISE NOTICE 'Successfully truncated table %', ctecurrentclinical_partition;

-- Insert data into cte_current_clinical
EXECUTE FORMAT('
INSERT INTO expanded_radet_client.%I
SELECT DISTINCT ON (tvs.person_uuid) 
tvs.person_uuid AS person_uuid10,tvs.ods_datim_id AS clin_ods_datim_id,
tvs.body_weight AS currentWeight,tbs.display AS tbStatus1,
bac.display AS currentClinicalStage,
CASE WHEN INITCAP(pp.sex) = ''Male'' THEN NULL
WHEN preg.display IS NOT NULL THEN preg.display 
ELSE hac.pregnancy_status END AS pregnancyStatus, 
CASE WHEN hac.tb_screen IS NOT NULL THEN hac.visit_date ELSE NULL END AS dateOfTbScreened1
FROM (SELECT uuid,person_uuid,ods_datim_id,body_weight,capture_date
FROM (SELECT uuid,person_uuid,ods_datim_id,body_weight,capture_date,
ROW_NUMBER() OVER (PARTITION BY person_uuid ORDER BY capture_date DESC) rowNumtvs
FROM public.%I WHERE archived=0) U WHERE rowNumtvs=1) tvs
INNER JOIN (SELECT vital_sign_uuid,person_uuid,ods_datim_id,clinical_stage_id,visit_date,
pregnancy_status,tb_status,tb_screen
FROM (SELECT vital_sign_uuid,person_uuid,ods_datim_id,clinical_stage_id,visit_date,
pregnancy_status,tb_status,tb_screen,
ROW_NUMBER() OVER (PARTITION BY person_uuid ORDER BY visit_date DESC) rowNum
FROM public.%I WHERE archived=0 AND visit_date < %L) Z WHERE rowNum=1) hac
ON tvs.uuid = hac.vital_sign_uuid AND tvs.person_uuid = hac.person_uuid
LEFT JOIN public.%I pp ON tvs.person_uuid = pp.uuid
INNER JOIN public.%I he ON he.person_uuid = hac.person_uuid
LEFT JOIN public.%I bac ON bac.id = hac.clinical_stage_id
LEFT JOIN public.%I preg ON preg.code = hac.pregnancy_status
LEFT JOIN public.%I tbs 
ON tbs.id = CASE WHEN hac.tb_status ~ ''^[0-9]+$'' THEN CAST(hac.tb_status AS INTEGER) ELSE 0 END
WHERE he.archived=0 AND pp .archived=0',
 ctecurrentclinical_partition,
triagevitalsign_partition,hivartclinical_partition,period_end_date,
patientperson_partition,hivenrollment_partition,
baseappcodeset_partition, baseappcodeset_partition, baseappcodeset_partition);

-- Log end time
SELECT TIMEOFDAY() INTO end_time;

-- Record the operation in the monitoring table
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctecurrentclinical_partition,start_time,end_time,datim_id);

END;
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_current_clinical(character varying)
    OWNER TO lamisplus_etl;

-- CALL expanded_radet_client.proc_current_clinical('A4VsXrMgMv4');