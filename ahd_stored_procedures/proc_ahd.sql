-- PROCEDURE: ahd.proc_ahd(character varying)

-- DROP PROCEDURE IF EXISTS ahd.proc_ahd(character varying);

CREATE OR REPLACE PROCEDURE ahd.proc_ahd(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE cteahd_partition text;
DECLARE patientperson_partition text;
DECLARE hivenrollment_partition text;
DECLARE laboratoryresult_partition text;
DECLARE laboratorytest_partition text;
DECLARE hivartclinical_partition text;
DECLARE baseappcodeset_partition text;
DECLARE triagevitalsign_partition text;
DECLARE hivstatustracker_partition text;
DECLARE hivregimen_partition text;
DECLARE hivregimentype_partition text;
DECLARE ahdmonitoringpartition text;
DECLARE period_end_date DATE;
DECLARE period_start_date DATE;


BEGIN

SELECT start_date
INTO period_start_date
FROM ahd.period WHERE is_active;

SELECT date
INTO period_end_date
FROM ahd.period WHERE is_active;

cteahd_partition := CONCAT('cte_ahd_',datim_id);
patientperson_partition := CONCAT('patient_person_',datim_id);
hivenrollment_partition := CONCAT('hiv_enrollment_',datim_id);
laboratoryresult_partition := CONCAT('laboratory_result_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
hivartclinical_partition := CONCAT('hiv_art_clinical_',datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_',datim_id);
triagevitalsign_partition := CONCAT('triage_vital_sign_',datim_id);
hivstatustracker_partition := CONCAT('hiv_status_tracker_',datim_id);
ahdmonitoringpartition := CONCAT('ahd_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE ahd.%I',cteahd_partition);

RAISE NOTICE 'successfully truncate % table', cteahd_partition;

EXECUTE FORMAT('INSERT INTO ahd.%I
SELECT DISTINCT ON (p.uuid) p.uuid AS PersonUuid,p.id,''******'' AS hospitalNumber,
''******'' AS surname,''******'' AS firstName,
COALESCE(he.date_of_registration, he.date_started) AS hivEnrollmentDate,
(EXTRACT(YEAR FROM AGE(CAST(%L AS DATE), COALESCE(he.date_of_registration, he.date_started))) * 12) 
+ EXTRACT(MONTH FROM AGE(CAST(%L AS DATE), COALESCE(he.date_of_registration, he.date_started))) AS ArvInterval,
EXTRACT(YEAR FROM AGE(CAST(%L AS DATE), p.date_of_birth)) AS age,
''******'' AS otherName,p.sex AS sex,p.date_of_birth AS dateOfBirth,p.date_of_registration AS dateOfRegistration, 
p.marital_status::jsonb->>''display'' AS maritalStatus,education::jsonb->>''display'' AS education, 
p.employment_status::jsonb->>''display'' AS occupation,cpm.facility_name AS facilityName, 
cpm.facility_lga AS lga,cpm.facility_state AS state,p.ods_datim_id AS datimId,testType.date_of_tb_diagnostic_result_received, 
testType.tb_diagnostic_test_type,testType.tb_diagnostic_result,lastVisit.visit_date,lastVisit.staging,
COALESCE(lastVisit.stage1Option, lastVisit.stage2Option, lastVisit.stage3Option, lastVisit.stage4Option) AS diseaseCondition,
weight.body_weight,he.date_of_registration AS Date_of_HIV_diagnosis,'''' AS treatmentDate,'''' AS preventingSymptoms,
(CASE WHEN (EXTRACT(YEAR FROM AGE(CAST(%L AS DATE), p.date_of_birth)) <= 5 OR lastVisit.staging IN (''STAGE III'', ''STAGE IV'')) 
THEN ''Yes'' ELSE ''No'' 
END) AS ahdStatus
FROM public.%I p
INNER JOIN central_partner_mapping cpm on p.ods_datim_id=cpm.datim_id
LEFT JOIN public.%I he ON he.person_uuid = p.uuid
LEFT JOIN (WITH cur_tb AS (SELECT sm.patient_uuid,sm.result_reported AS tb_diagnostic_result,
CAST(sm.date_result_reported AS DATE) AS date_of_tb_diagnostic_result_received,
CASE lt.lab_test_id 
WHEN 65 THEN ''Gene Xpert'' 
WHEN 51 THEN ''TB-LAM''
WHEN 66 THEN ''Chest X-ray'' 
WHEN 64 THEN ''AFB microscopy'' 
WHEN 67 THEN ''Gene Xpert'' 
WHEN 58 THEN ''TB-LAM'' 
WHEN 50 THEN ''Visitect CD4'' 
WHEN 71 THEN ''LF-LAM''
WHEN 52 THEN ''Cryptococcal Antigen''
WHEN 69 THEN ''Serum crAg'' 
WHEN 70 THEN ''CSF crAg'' END AS tb_diagnostic_test_type, 
ROW_NUMBER() OVER (PARTITION BY sm.patient_uuid ORDER BY sm.date_result_reported DESC) AS rnk 
FROM public.%I sm 
INNER JOIN public.%I lt ON sm.test_id = lt.id AND sm.patient_uuid=lt.patient_uuid
WHERE lt.lab_test_id IN (50, 71, 70, 65, 51, 66, 64, 69) 
AND sm.archived = 0 AND sm.date_result_reported IS NOT NULL
AND sm.date_result_reported <= CAST(%L AS date)) 
SELECT patient_uuid, tb_diagnostic_result, date_of_tb_diagnostic_result_received, tb_diagnostic_test_type 
FROM cur_tb WHERE rnk = 1
) testType ON testType.patient_uuid = p.uuid

LEFT JOIN (
SELECT person_uuid,visit_date,bac.display AS staging,(who::jsonb)->>''stage1ValueOption'' AS stage1Option, 
(who::jsonb)->>''stage2ValueOption'' AS stage2Option,(who::jsonb)->>''stage3ValueOption'' AS stage3Option, 
(who::jsonb)->>''stage4ValueOption'' AS stage4Option, 
ROW_NUMBER() OVER (PARTITION BY person_uuid ORDER BY visit_date DESC) AS rnk 
FROM public.%I hac
LEFT JOIN public.%I bac ON bac.id = hac.clinical_stage_id
WHERE hac.visit_date BETWEEN CAST(%L AS DATE) AND CAST(%L AS DATE)
) lastVisit ON lastVisit.person_uuid = p.uuid AND lastVisit.rnk = 1

LEFT JOIN (
SELECT person_uuid, body_weight, capture_date,
ROW_NUMBER() OVER (PARTITION BY person_uuid ORDER BY capture_date DESC) AS rnk 
FROM public.%I 
WHERE archived = 0 AND capture_date BETWEEN CAST(%L AS DATE) AND CAST(%L AS DATE)
) weight ON weight.person_uuid = p.uuid AND weight.rnk = 1

LEFT JOIN (
SELECT person_id, hiv_status, status_date, 
ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY status_date DESC) AS rnk 
FROM public.%I 
WHERE hiv_status IS NOT NULL AND hiv_status != '''' 
AND hiv_status != ''HIV_NEGATIVE'') currentStatus ON currentStatus.person_id = p.uuid AND currentStatus.rnk = 1
WHERE p.archived = 0 
AND p.date_of_registration BETWEEN CAST(%L AS DATE) AND CAST(%L AS DATE)',
cteahd_partition,period_end_date,period_end_date,period_end_date,period_end_date,
patientperson_partition,hivenrollment_partition,laboratoryresult_partition,
laboratorytest_partition,period_end_date,hivartclinical_partition,
baseappcodeset_partition,period_start_date,period_end_date,
triagevitalsign_partition,period_start_date,period_end_date,
hivstatustracker_partition,period_start_date,period_end_date);

SELECT TIMEOFDAY() INTO end_time;

EXECUTE FORMAT('INSERT INTO ahd.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
ahdmonitoringpartition,cteahd_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE ahd.proc_ahd(character varying)
    OWNER TO lamisplus_etl;

-- CALL ahd.proc_ahd('A1xxdELs2fm');
-- SELECT * FROM ahd.ahd_monitoring;