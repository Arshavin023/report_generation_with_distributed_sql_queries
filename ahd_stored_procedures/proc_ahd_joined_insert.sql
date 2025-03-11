-- PROCEDURE: ahd.proc_ahd_joined_insert(character varying)

-- DROP PROCEDURE IF EXISTS ahd.proc_ahd_joined_insert(character varying);

CREATE OR REPLACE PROCEDURE ahd.proc_ahd_joined_insert(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ahdjoinedpartition TEXT;
DECLARE cteahdpartition TEXT;
DECLARE ctecarecd4partition TEXT;
DECLARE ctecurrentstatuspartition TEXT;
DECLARE ctepreviousstatuspartition TEXT;
DECLARE ctecurrentvlresultpartition TEXT;
DECLARE ctelastoneyearvlresultpartition TEXT;
DECLARE cteeacpartition TEXT;
DECLARE ctelabcd4partition TEXT;
DECLARE ctelastcd4partition TEXT;
DECLARE ctecd4typepartition TEXT;
DECLARE ctesamplecollectiondatepartition TEXT;
DECLARE ctelastlflampartition TEXT;
DECLARE ctelastcrytococalantigenpartition TEXT;
DECLARE ctelastserumcragpartition TEXT;
DECLARE ctelastcsfcragpartition TEXT;
DECLARE ctelastvisitectpartition TEXT;
DECLARE ahdmonitoringpartition text;

BEGIN

ahdjoinedpartition := CONCAT('ahd_joined_',datim_id);
cteahdpartition := CONCAT('cte_ahd_', datim_id);
ctecarecd4partition := CONCAT('cte_carecardcd4_', datim_id);
ctecd4typepartition := CONCAT('cte_cd4type_', datim_id);
ctecurrentstatuspartition := CONCAT('cte_current_status_', datim_id);
ctepreviousstatuspartition := CONCAT('cte_previous_', datim_id);
ctecurrentvlresultpartition := CONCAT('cte_current_vl_result_', datim_id);
ctelastoneyearvlresultpartition := CONCAT('cte_lastoneyear_vl_result_', datim_id);
cteeacpartition := CONCAT('cte_eac_', datim_id);
ctelabcd4partition := CONCAT('cte_labcd4_', datim_id);
ctelastcd4partition := CONCAT('cte_lastcd4_', datim_id);
ctesamplecollectiondatepartition := CONCAT('cte_sample_collection_date_', datim_id);
ctelastlflampartition := CONCAT('cte_lastlflam_', datim_id);
ctelastcrytococalantigenpartition := CONCAT('cte_lastcrytococalantigen_', datim_id);
ctelastserumcragpartition:= CONCAT('cte_lastserumcrag_', datim_id);
ctelastcsfcragpartition := CONCAT('cte_lastcsfcrag_', datim_id);
ctelastvisitectpartition := CONCAT('cte_lastvisitect_', datim_id);
ahdmonitoringpartition := CONCAT('ahd_monitoring_',datim_id);


SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE ahd.%I',ahdjoinedpartition);

RAISE NOTICE 'Successfully truncated % table', ahdjoinedpartition;
RAISE NOTICE 'cteahdpartition: %', cteahdpartition;
RAISE NOTICE 'ctecurrentvlresultpartition: %', ctecurrentvlresultpartition;

EXECUTE FORMAT('INSERT INTO ahd.%I
SELECT ahd.state,ahd.lga,ahd.facilityName,ahd.datimId,ahd.PersonUuid,ahd.hospitalNumber,
ahd.sex,ahd.dateOfBirth,ahd.age,ahd.body_weight,
ahd.Date_of_HIV_diagnosis,ahd.hivEnrollmentDate,
(CASE WHEN previous.status ILIKE ''%%DEATH%%'' THEN ''DEATH''
WHEN previous.status ILIKE ''%%out%%'' THEN ''TRANSFER OUT''
WHEN previous.status ILIKE ''%%Died%%'' THEN ''DEATH''
WHEN previous.status ILIKE ''%%NON_ART%%'' THEN ''NON ART''
WHEN previous.status ILIKE ''%%invalid%%'' THEN ''INVALID''
ELSE previous.status END) AS previous_status,
previous.status_date previous_status_date,
(CASE
WHEN currentStatus.status ILIKE ''%%DEATH%%'' THEN ''DEATH''
WHEN currentStatus.status ILIKE ''%%out%%'' THEN ''TRANSFER OUT''
WHEN currentStatus.status ILIKE ''%%Died%%'' THEN ''DEATH''
WHEN currentStatus.status ILIKE ''%%NON_ART%%'' THEN ''NON ART''
WHEN currentStatus.status ILIKE ''%%invalid%%'' THEN ''INVALID''
WHEN (previous.status ILIKE ''%%IIT%%'' OR previous.status ILIKE ''%%stop%%'' ) AND (currentStatus.status ILIKE ''%%ACTIVE%%'') THEN ''Active Restart''
ELSE currentStatus.status END) current_Status, 
currentStatus.status_date current_status_date,
vl_result.currentViralLoad,
vl_result.dateOfCurrentViralLoad,
vl_result.viralLoadIndication,
eac.dateOfRepeatViralLoadEACSampleCollection, 
eac.repeatViralLoadResult, 
eac.DateOfRepeatViralLoadResult,
(CASE 
WHEN (currentStatus.status = ''Active'' AND ArvInterval <= 6) THEN ''Newly Diagnosed Client''
WHEN ((currentStatus.status = ''Active'' OR (previous.status ILIKE ''%%IIT%%'' OR previous.status ILIKE ''%%stop%%'' ) AND (currentStatus.status ILIKE ''%%ACTIVE%%'')) AND ahd.age < 5 ) THEN ''PLHIV <5 years''
WHEN ((previous.status ILIKE ''%%IIT%%'' OR previous.status ILIKE ''%%stop%%'' ) AND (currentStatus.status ILIKE ''%%ACTIVE%%'')) THEN ''Restarted on ART after Interruption''
WHEN ((eac.dateOfRepeatViralLoadEACSampleCollection > vl_result.dateOfCurrentViralLoadSample) AND currentStatus.status = ''Active'' AND ArvInterval > 6 AND ahd.age > 5 AND (vl_result.currentViralLoad ~ ''^\d+$'' AND CAST(vl_result.currentViralLoad AS INT) >= 1000) AND eac.repeatViralLoadResult ~ ''^\d+$'' AND CAST(eac.repeatViralLoadResult AS INT) >= 1000 AND ((EXTRACT(YEAR FROM AGE(vl_result.dateOfCurrentViralLoadSample, ahd.hivEnrollmentDate)) * 12) + EXTRACT(MONTH FROM AGE(vl_result.dateOfCurrentViralLoadSample, ahd.hivEnrollmentDate))> 3) ) THEN ''Unsuppressed client post EAC''
WHEN ((vl_result.currentViralLoad ~ ''^\d+$'' AND CAST(vl_result.currentViralLoad AS INT) BETWEEN 50 AND 999) AND ahd.age < 5 AND ArvInterval > 6 AND currentStatus.status = ''Active'') THEN ''Persistent low level viraemia''
END) category,
ahd.visit_date, 
ahd.staging,
ahd.diseaseCondition,
ahd.preventingSymptoms,
ahd.ahdStatus,
lastcd4.lastCd4Count,
lastcd4.dateOfLastCd4Count,
cd4Type.cd4_type,
lastCrytococalAntigen.lastCrytococalAntigen,
lastCrytococalAntigen.dateOfLastCrytococalAntigen,
lastLfLam.lastLfLam,
lastLfLam.dateOfLastLfLam,
lastSerumCrAg.lastSerumCrAg,
lastSerumCrAg.dateOfLastSerumCrAg,
lastCSFCrAg.lastCSFCrAg,
lastCSFCrAg.dateOfLastCSFCrAg,
ahd.treatmentDate

FROM ahd.%I ahd
LEFT JOIN ahd.%I vl_result ON ahd.PersonUuid = vl_result.person_uuid130
LEFT JOIN ahd.%I lastcd4 ON lastcd4.lastcd4_person_uuid = ahd.PersonUuid
LEFT JOIN ahd.%I lastCrytococalAntigen ON lastCrytococalAntigen.personuuid12 = ahd.PersonUuid
LEFT JOIN ahd.%I lastSerumCrAg ON lastSerumCrAg.lastSerumCrAg_personuuid = ahd.PersonUuid
LEFT JOIN ahd.%I lastCSFCrAg ON lastCSFCrAg.lastCSFCrAg_personuuid = ahd.PersonUuid
LEFT JOIN ahd.%I lastVisitect ON lastVisitect.lastVisitect_personuuid = ahd.PersonUuid
LEFT JOIN ahd.%I lastLfLam ON lastLfLam.lastLfLam_personuuid = ahd.PersonUuid
LEFT JOIN ahd.%I eac ON eac.person_Uuid50 = ahd.PersonUuid
LEFT JOIN ahd.%I previous ON previous.prePersonUuid = ahd.PersonUuid
LEFT JOIN ahd.%I currentStatus ON currentStatus.cuPersonUuid = ahd.PersonUuid 
LEFT JOIN ahd.%I cd4Type ON cd4Type.cd4Type_person_uuid = ahd.PersonUuid
LEFT JOIN ahd.%I viralLoad ON viralLoad.lastoneyear_person_uuid = ahd.PersonUuid
WHERE
CASE WHEN (currentStatus.status = ''Active'' AND ArvInterval <= 6) THEN ''Newly Diagnosed Client''
WHEN ((currentStatus.status = ''Active'' OR (previous.status ILIKE ''%%IIT%%'' 
	OR previous.status ILIKE ''%%stop%%'' ) AND (currentStatus.status ILIKE ''%%ACTIVE%%'')) AND ahd.age < 5 ) THEN ''PLHIV <5 years''
WHEN ((previous.status ILIKE ''%%IIT%%'' OR previous.status ILIKE ''%%stop%%'' ) AND (currentStatus.status ILIKE ''%%ACTIVE%%'')) THEN ''Restarted on ART after Interruption''
WHEN ((eac.dateOfRepeatViralLoadEACSampleCollection > vl_result.dateOfCurrentViralLoadSample) AND currentStatus.status = ''Active'' 
	AND ArvInterval > 6 AND ahd.age > 5 
	AND (vl_result.currentViralLoad ~ ''^\d+$'' 
	AND CAST(vl_result.currentViralLoad AS INT) >= 1000) 
	AND eac.repeatViralLoadResult ~ ''^\d+$'' 
	AND CAST(eac.repeatViralLoadResult AS INT) >= 1000 
	AND ((EXTRACT(YEAR FROM AGE(vl_result.dateOfCurrentViralLoadSample, ahd.hivEnrollmentDate)) * 12) + EXTRACT(MONTH FROM AGE(vl_result.dateOfCurrentViralLoadSample, ahd.hivEnrollmentDate))> 3) ) THEN ''Unsuppressed client post EAC''
WHEN ((vl_result.currentViralLoad ~ ''^\d+$'' AND CAST(vl_result.currentViralLoad AS INT) BETWEEN 50 AND 999) AND ahd.age < 5 AND ArvInterval > 6 AND currentStatus.status = ''Active'') THEN ''Persistent low level viraemia''
END != ''''
',ahdjoinedpartition,cteahdpartition,ctecurrentvlresultpartition,ctelastcd4partition,
ctelastcrytococalantigenpartition,ctelastserumcragpartition,ctelastcsfcragpartition,
ctelastvisitectpartition,ctelastlflampartition,cteeacpartition,
ctepreviousstatuspartition,ctecurrentstatuspartition,ctecd4typepartition,
ctelastoneyearvlresultpartition);

SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO ahd.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
ahdmonitoringpartition,ahdjoinedpartition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE ahd.proc_ahd_joined_insert(character varying)
    OWNER TO lamisplus_etl;

-- CALL ahd.proc_ahd_joined_insert('A1xxdELs2fm');

-- SELECT * FROM ahd.ahd_monitoring;