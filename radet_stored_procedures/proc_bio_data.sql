-- PROCEDURE: expanded_radet_client.proc_bio_data(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_bio_data(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_bio_data(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctebiodata_partition text;
DECLARE patientperson_partition text;
DECLARE hivenrollment_partition text;
DECLARE baseappcodeset_partition text;
DECLARE hivartclinical_partition text;
DECLARE hivregimen_partition text;
DECLARE hivregimentype_partition text;
DECLARE radetmonitoringpartition text;
DECLARE period_end_date DATE;

BEGIN
SELECT date
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctebiodata_partition := CONCAT('cte_bio_data_',datim_id);
patientperson_partition := CONCAT('patient_person_',datim_id);
hivenrollment_partition := CONCAT('hiv_enrollment_',datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_',datim_id);
hivartclinical_partition := CONCAT('hiv_art_clinical_',datim_id);
hivregimen_partition := CONCAT('hiv_regimen_',datim_id);
hivregimentype_partition := CONCAT('hiv_regimen_type_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctebiodata_partition);

RAISE NOTICE 'successfully truncate % table', ctebiodata_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT DISTINCT on (p.uuid, p.ods_datim_id) p.uuid AS personUuid,
p.ods_datim_id AS bio_ods_datim_id,
p.hospital_number AS hospitalNumber,
h.unique_id as uniqueId,
EXTRACT(YEAR FROM  AGE(CAST(%L AS DATE), date_of_birth)) AS age,
INITCAP(p.sex) AS gender,
p.date_of_birth AS dateOfBirth,
cpm.facility_name AS facilityName,
cpm.facility_lga AS lga,
cpm.facility_state AS state,
tgroup.display AS targetGroup,
eSetting.display AS enrollmentSetting,
hac.visit_date AS artStartDate,
hr.description AS regimenAtStart,
p.date_of_registration as dateOfRegistration,
h.date_of_registration as dateOfEnrollment,
h.ovc_number AS ovcUniqueId,
h.house_hold_number AS householdUniqueNo,
ecareEntry.display AS careEntry,
hrt.description AS regimenLineAtStart
FROM public.%I p
INNER JOIN central_partner_mapping cpm on p.ods_datim_id=cpm.datim_id
INNER JOIN public.%I h ON h.person_uuid = p.uuid
LEFT JOIN public.%I tgroup ON tgroup.id = h.target_group_id
LEFT JOIN public.%I eSetting ON eSetting.id = h.enrollment_setting_id
LEFT JOIN public.%I ecareEntry ON ecareEntry.id = h.entry_point_id
INNER JOIN public.%I hac ON hac.hiv_enrollment_uuid = h.uuid
INNER JOIN public.%I hr ON hr.id = hac.regimen_id
INNER JOIN public.%I hrt ON hrt.id = hac.regimen_type_id 
AND hac.regimen_type_id IN (1,2,3,4,14, 16)
WHERE h.archived=0 AND hac.is_commencement = TRUE AND p.ods_datim_id is not null
AND hac.visit_date >= ''1980-01-01''
AND hac.visit_date < %L',
ctebiodata_partition,period_end_date,
patientperson_partition,hivenrollment_partition,
baseappcodeset_partition,baseappcodeset_partition,baseappcodeset_partition,
hivartclinical_partition,hivregimen_partition,hivregimentype_partition,
period_end_date)
;

SELECT TIMEOFDAY() INTO end_time;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctebiodata_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_bio_data(character varying)
    OWNER TO lamisplus_etl;

-- CALL expanded_radet_client.proc_bio_data('tZy8wIM53xT');

