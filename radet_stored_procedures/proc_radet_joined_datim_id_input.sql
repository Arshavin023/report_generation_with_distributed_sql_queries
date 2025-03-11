-- PROCEDURE: expanded_radet_client.proc_radet_joined_insert(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_radet_joined_insert(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_radet_joined_insert(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE radetjoinedpartition TEXT;
DECLARE ctebiodapartition TEXT;
DECLARE ctebiometricpartition TEXT;
DECLARE ctecarecd4partition TEXT;
DECLARE ctecasemanagerpartition TEXT;
DECLARE ctecervicalcancerpartition TEXT;
DECLARE cteclientverificationpartition TEXT;
DECLARE ctecrytococalpartition TEXT;
DECLARE ctecurrentclinicalpartition TEXT;
DECLARE ctecurrentregimenpartition TEXT;
DECLARE ctecurrentstatuspartition TEXT;
DECLARE ctepreviousstatuspartition TEXT;
DECLARE ctepreviouspreviouspartition TEXT;
DECLARE ctecurrenttbresultpartition TEXT;
DECLARE ctecurrentvlresultpartition TEXT;
DECLARE ctedsd1partition TEXT;
DECLARE ctedsd2partition TEXT;
DECLARE cteeacpartition TEXT;
DECLARE cteiptpartition TEXT;
DECLARE cteiptspartition TEXT;
DECLARE cteiptnewpartition TEXT;
DECLARE ctelabcd4partition TEXT;
DECLARE ctenaivevldatapartition TEXT;
DECLARE cteovcpartition TEXT;
DECLARE ctepatientlgapartition TEXT;
DECLARE ctepharmacydetailsregimenpartition TEXT;
DECLARE ctesamplecollectiondatepartition TEXT;
DECLARE ctetbsamplecollectionpartition TEXT;
DECLARE ctetblampartition TEXT;
DECLARE ctetbstatuspartition TEXT;
DECLARE ctetbtreatmentpartition TEXT;
DECLARE ctetbtreatmentnewpartition TEXT;
DECLARE ctevacauseofdeathpartition TEXT;
DECLARE radetmonitoringpartition text;

BEGIN

radetjoinedpartition := CONCAT('obt_radet_',datim_id);
ctebiodapartition := CONCAT('cte_bio_data_', datim_id);
ctebiometricpartition := CONCAT('cte_biometric_', datim_id);
ctecarecd4partition := CONCAT('cte_carecardcd4_', datim_id);
ctecasemanagerpartition := CONCAT('cte_case_manager_', datim_id);
ctecervicalcancerpartition := CONCAT('cte_cervical_cancer_', datim_id);
cteclientverificationpartition := CONCAT('cte_client_verification_', datim_id);
ctecrytococalpartition := CONCAT('cte_crytococal_antigen_', datim_id);
ctecurrentclinicalpartition := CONCAT('cte_current_clinical_', datim_id);
ctecurrentregimenpartition := CONCAT('cte_current_regimen_', datim_id);
ctecurrentstatuspartition := CONCAT('cte_current_status_', datim_id);
ctepreviousstatuspartition := CONCAT('cte_previous_', datim_id);
ctepreviouspreviouspartition := CONCAT('cte_previous_previous_', datim_id);
ctecurrenttbresultpartition := CONCAT('cte_current_tb_result_', datim_id);
ctecurrentvlresultpartition := CONCAT('cte_current_vl_result_', datim_id);
ctedsd1partition := CONCAT('cte_dsd1_', datim_id);
ctedsd2partition := CONCAT('cte_dsd2_', datim_id);
cteeacpartition := CONCAT('cte_eac_', datim_id);
cteiptpartition := CONCAT('cte_ipt_', datim_id);
cteiptspartition := CONCAT('cte_ipt_s_', datim_id);
cteiptnewpartition := CONCAT('cte_iptnew_', datim_id);
ctelabcd4partition := CONCAT('cte_labcd4_', datim_id);
ctenaivevldatapartition := CONCAT('cte_naive_vl_data_', datim_id);
cteovcpartition := CONCAT('cte_ovc_', datim_id);
ctepatientlgapartition := CONCAT('cte_patient_lga_', datim_id);
ctepharmacydetailsregimenpartition := CONCAT('cte_pharmacy_details_regimen_', datim_id);
ctesamplecollectiondatepartition := CONCAT('cte_sample_collection_date_', datim_id);
ctetbsamplecollectionpartition := CONCAT('cte_tb_sample_collection_', datim_id);
ctetblampartition := CONCAT('cte_tblam_', datim_id);
ctetbstatuspartition := CONCAT('cte_tbstatus_', datim_id);
ctetbtreatmentpartition := CONCAT('cte_tbtreatment_', datim_id);
ctetbtreatmentnewpartition := CONCAT('cte_tbtreatmentnew_', datim_id);
ctevacauseofdeathpartition := CONCAT('cte_vacauseofdeath_', datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet.%I',radetjoinedpartition);

RAISE NOTICE 'Successfully truncated % table', radetjoinedpartition;

EXECUTE FORMAT('INSERT INTO expanded_radet.%I
SELECT DISTINCT ON (bd.personUuid) personUuid AS uniquePersonUuid,
bd.personUuid,bd.bio_ods_datim_id,bd.hospitalnumber,bd.uniqueid,
bd.age,bd.gender,bd.dateofbirth,bd.facilityname,bd.lga,bd.state,bd.targetgroup,
bd.enrollmentsetting,CAST(bd.artstartdate AS DATE),bd.regimenatstart,
bd.dateofregistration,bd.dateofenrollment,bd.ovcuniqueid,bd.householduniqueno,
bd.careentry,bd.regimenlineatstart,
CONCAT(bd.bio_ods_datim_id, ''_'', bd.personUuid) AS ndrPatientIdentifier, 
p_lga.stateofresidence,p_lga.lgaofresidence,
scd.dateofviralloadsamplecollection,
cvlr.dateofcurrentviralloadsample,cvlr.vlfacility,cvlr.vlarchived,cvlr.viralloadindication,
cvlr.currentviralload, cvlr.dateofcurrentviralload,
pdr.dsdmodel,pdr.lastpickupdate,pdr.currentartregimen,pdr.currentregimenline,
pdr.nextpickupdate,pdr.monthsofarvrefill,
b.datebiometricsenrolled,b.numberoffingerscaptured,
b.datebiometricsrecaptured,b.numberoffingersrecaptured,b.biometricstatus,
b.status_date,
c.currentweight,c.tbstatus1,c.currentclinicalstage,c.pregnancystatus,c.dateoftbscreened1,
e.dateofcommencementofeac,e.dateoflasteacsessioncompleted,e.numberofeacsessioncompleted,
e.dateofextendeaccompletion,e.repeatviralloadresult,e.dateofrepeatviralloadresult,
e.dateofrepeatviralloadeacsamplecollection,
ca.DateOfStartOfCurrentARTRegimen,
ca.person_uuid70,
iptStart.dateOfIptStart AS dateOfIptStart,
COALESCE(CAST (iptN.tptCompletionDate AS DATE), CAST(ipt.iptCompletionDate AS DATE)) AS iptCompletionDate, 
(CASE WHEN COALESCE(iptN.tptCompletionStatus, ipt.iptCompletionStatus) = ''IPT Completed''
THEN ''Treatment completed'' ELSE COALESCE(iptN.tptCompletionStatus, ipt.iptCompletionStatus)
END) AS iptCompletionStatus,
iptStart.iptType AS iptType,
cc.dateofcervicalcancerscreening,cc.treatmentmethoddate,cc.cervicalcancerscreeningtype,
cc.cervicalcancerscreeningmethod,cc.cervicalcancertreatmentscreened,cc.resultofcervicalcancerscreening,
dsd1.dateofdevolvement,dsd1.modeldevolvedto,
dsd2.dateofcurrentdsd,dsd2.currentdsdmodel,dsd2.datereturntosite,dsd2.currentdsdoutlet,
ov.ovcnumber,ov.householdnumber,
(CASE WHEN COALESCE(tbTmentNew.tbTreatmentType, tbTment.tbTreatementType) IN (''New'', ''Relapse'', ''Relapsed'') THEN ''New/Relapse'' ELSE COALESCE(tbTmentNew.tbTreatmentType, tbTment.tbTreatementType) END)  AS tbTreatementType,
COALESCE(tbTmentNew.tbTreatmentStartDate, tbTment.tbTreatmentStartDate) AS tbTreatmentStartDate,
COALESCE(tbTmentNew.treatmentOutcome, tbTment.tbTreatmentOutcome) AS tbTreatmentOutcome,
COALESCE(tbTmentNew.completionDate, tbTment.tbCompletionDate) AS tbCompletionDate,
COALESCE(tbTmentNew.person_uuid_tbnew, tbTment.tbTreatmentPersonUuid) AS tbTreatmentPersonUuid,
tbSample.created_by,tbSample.dateoftbsamplecollection,
tbResult.persontbresult,tbResult.dateoftbdiagnosticresultreceived,
tbResult.tbdiagnosticresult,tbResult.tbdiagnostictesttype,
tbS.tbstatus,tbS.dateoftbscreened,tbS.tbscreeningtype,
tbl.dateoflasttblam,tbl.tblamresult,
crypt.dateoflastcrytococalantigen, crypt.lastcrytococalantigen,
COALESCE (vaod.cause_of_death, ct.cause_of_death) AS causeOfDeath,
COALESCE (vaod.va_cause_of_death, ct.va_cause_of_death) AS vaCauseOfDeath,

(CASE WHEN prepre.status ILIKE ''%%DEATH%%'' THEN ''Died''
WHEN prepre.status ILIKE ''%%out%%'' THEN ''Transferred Out''
WHEN pre.status ILIKE ''%%DEATH%%'' THEN ''Died''
WHEN pre.status ILIKE ''%%out%%'' THEN ''Transferred Out''
WHEN (prepre.status ILIKE ''%%IIT%%'' OR prepre.status ILIKE ''%%stop%%'')
AND (pre.status ILIKE ''%%ACTIVE%%'') THEN ''Active Restart''
WHEN prepre.status ILIKE ''%%ACTIVE%%''
AND pre.status ILIKE ''%%ACTIVE%%'' THEN ''Active''
ELSE REPLACE(pre.status, ''_'', '' '')
END) AS previousStatus,

CAST((CASE WHEN prepre.status ILIKE ''%%DEATH%%'' THEN prepre.status_date
WHEN prepre.status ILIKE ''%%out%%'' THEN prepre.status_date
WHEN pre.status ILIKE ''%%DEATH%%'' THEN pre.status_date
WHEN pre.status ILIKE ''%%out%%'' THEN pre.status_date
WHEN (prepre.status ILIKE ''%%IIT%%'' OR prepre.status ILIKE ''%%stop%%'')
AND (pre.status ILIKE ''%%ACTIVE%%'') THEN pre.status_date
WHEN prepre.status ILIKE ''%%ACTIVE%%''
AND pre.status ILIKE ''%%ACTIVE%%'' THEN pre.status_date
ELSE pre.status_date
END) AS DATE)AS previousStatusDate,

(CASE WHEN ((pre.status ILIKE ''%%IIT%%'' OR pre.status ILIKE ''%%stop%%'') 
AND (ct.status ILIKE ''%%ACTIVE%%'')) THEN ''Active Restart''
WHEN ct.status ILIKE ''%%ACTIVE%%'' THEN ''Active''
WHEN ct.status ILIKE ''%%ART Transfer In%%'' THEN ''''
WHEN prepre.status ILIKE ''%%DEATH%%'' THEN ''Died''
WHEN prepre.status ILIKE ''%%out%%'' THEN ''Transferred Out''
WHEN pre.status ILIKE ''%%DEATH%%'' THEN ''Died''
WHEN pre.status ILIKE ''%%out%%'' THEN ''Transferred Out''
WHEN ct.status ILIKE ''%%IIT%%'' THEN ''IIT''
WHEN ct.status ILIKE ''%%out%%'' THEN ''Transferred Out''
WHEN ct.status ILIKE ''%%DEATH%%'' THEN ''Died''
WHEN pre.status ILIKE ''%%ACTIVE%%''
AND ct.status ILIKE ''%%ACTIVE%%'' THEN ''Active''
ELSE REPLACE(ct.status, ''_'', '' '')
END) AS currentStatus,

CAST((
CASE
WHEN ct.status ILIKE ''%%ACTIVE%%'' THEN ct.status_date
WHEN ct.status ILIKE ''%%ART Transfer In%%'' THEN ct.status_date
WHEN prepre.status ILIKE ''%%DEATH%%'' THEN prepre.status_date
WHEN prepre.status ILIKE ''%%out%%'' THEN prepre.status_date
WHEN pre.status ILIKE ''%%DEATH%%'' THEN pre.status_date
WHEN pre.status ILIKE ''%%out%%'' THEN pre.status_date
WHEN ct.status ILIKE ''%%IIT%%'' THEN
CASE
WHEN (pre.status ILIKE ''%%DEATH%%'' OR pre.status ILIKE ''%%out%%'' OR pre.status ILIKE ''%%stop%%'') THEN pre.status_date
ELSE ct.status_date --check the pre to see the status and return date appropriate
END
WHEN ct.status ILIKE ''%%stop%%'' THEN
CASE
WHEN (pre.status ILIKE ''%%DEATH%%'' OR pre.status ILIKE ''%%out%%'' OR pre.status ILIKE ''%%IIT%%'') THEN pre.status_date
ELSE ct.status_date --check the pre to see the status and return date appropriate
END
WHEN ct.status ILIKE ''%%out%%'' THEN
CASE
WHEN (pre.status ILIKE ''%%DEATH%%'' OR pre.status ILIKE ''%%stop%%'' OR pre.status ILIKE ''%%IIT%%'') THEN pre.status_date
ELSE ct.status_date --check the pre to see the status and return date appropriate
END
WHEN (
pre.status ILIKE ''%%IIT%%''
OR pre.status ILIKE ''%%stop%%''
)
AND (ct.status ILIKE ''%%ACTIVE%%'') THEN ct.status_date
WHEN pre.status ILIKE ''%%ACTIVE%%''
AND ct.status ILIKE ''%%ACTIVE%%'' THEN ct.status_date
ELSE ct.status_date
END
)AS DATE) AS currentStatusDate,
cvl.clientVerificationStatus, 
cvl.clientVerificationOutCome,
(
CASE
WHEN prepre.status ILIKE ''%%DEATH%%'' THEN FALSE
WHEN prepre.status ILIKE ''%%out%%'' THEN FALSE
WHEN pre.status ILIKE ''%%DEATH%%'' THEN FALSE
WHEN pre.status ILIKE ''%%out%%'' THEN FALSE
WHEN ct.status ILIKE ''%%IIT%%'' THEN FALSE
WHEN ct.status ILIKE ''%%out%%'' THEN FALSE
WHEN ct.status ILIKE ''%%DEATH%%'' THEN FALSE
WHEN ct.status ILIKE ''%%stop%%'' THEN FALSE
WHEN (nvd.age >= 15
AND nvd.regimen ILIKE ''%%DTG%%''
AND bd.artstartdate + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) 
AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'') THEN TRUE
WHEN (nvd.age >= 15
AND nvd.regimen NOT ILIKE ''%%DTG%%''
AND bd.artstartdate + 181 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'') THEN TRUE
WHEN (nvd.age <= 15 AND bd.artstartdate + 181 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'') THEN TRUE

WHEN CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) IS NULL
AND scd.dateofviralloadsamplecollection IS NULL AND
cvlr.dateofcurrentviralload IS NULL
AND CAST(bd.artstartdate AS DATE) + 181 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN TRUE

WHEN CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) IS NULL
AND scd.dateofviralloadsamplecollection IS NOT NULL AND
cvlr.dateofcurrentviralload IS NULL
AND CAST(bd.artstartdate AS DATE) + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN TRUE

WHEN CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) < 1000
AND( scd.dateofviralloadsamplecollection < cvlr.dateofcurrentviralload
OR  scd.dateofviralloadsamplecollection IS NULL )
AND CAST(cvlr.dateofcurrentviralload AS DATE) + 181 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN TRUE

WHEN  CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) < 1000
AND (scd.dateofviralloadsamplecollection > cvlr.dateofcurrentviralload
OR cvlr.dateofcurrentviralload IS NULL
)
AND CAST(scd.dateofviralloadsamplecollection AS DATE) + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN TRUE

WHEN CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) > 1000
AND ( scd.dateofviralloadsamplecollection < cvlr.dateofcurrentviralload
OR
scd.dateofviralloadsamplecollection IS NULL
)
AND CAST(cvlr.dateofcurrentviralload AS DATE) + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN TRUE

WHEN
CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) > 1000
AND (scd.dateofviralloadsamplecollection > cvlr.dateofcurrentviralload
OR cvlr.dateofcurrentviralload IS NULL)
AND CAST(scd.dateofviralloadsamplecollection AS DATE) + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN TRUE

ELSE FALSE
END
) AS vlEligibilityStatus,
CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) AS test,
(
CASE
WHEN prepre.status ILIKE ''%%DEATH%%'' THEN NULL
WHEN prepre.status ILIKE ''%%out%%'' THEN NULL
WHEN pre.status ILIKE ''%%DEATH%%'' THEN NULL
WHEN pre.status ILIKE ''%%out%%'' THEN NULL
WHEN ct.status ILIKE ''%%IIT%%'' THEN NULL
WHEN ct.status ILIKE ''%%out%%'' THEN NULL
WHEN ct.status ILIKE ''%%DEATH%%'' THEN NULL
WHEN ct.status ILIKE ''%%stop%%'' THEN NULL
WHEN (nvd.age >= 15
AND nvd.regimen ILIKE ''%%DTG%%''
AND bd.artstartdate + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'')
THEN CAST(bd.artstartdate + 91 AS DATE)
WHEN (nvd.age >= 15
AND nvd.regimen NOT ILIKE ''%%DTG%%''
AND bd.artstartdate + 181 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'')
THEN CAST(bd.artstartdate + 181 AS DATE)
WHEN (nvd.age <= 15 AND bd.artstartdate + 181 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'')
THEN CAST(bd.artstartdate + 181 AS DATE)

WHEN CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) IS NULL
AND scd.dateofviralloadsamplecollection IS NULL AND
cvlr.dateofcurrentviralload IS NULL
AND CAST(bd.artstartdate AS DATE) + 181 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN
CAST(bd.artstartdate AS DATE) + 181

WHEN CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) IS NULL
AND scd.dateofviralloadsamplecollection IS NOT NULL AND
cvlr.dateofcurrentviralload IS NULL
AND CAST(bd.artstartdate AS DATE) + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN
CAST(bd.artstartdate AS DATE) + 91

WHEN CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) < 1000
AND( scd.dateofviralloadsamplecollection < cvlr.dateofcurrentviralload
OR  scd.dateofviralloadsamplecollection IS NULL )
AND CAST(cvlr.dateofcurrentviralload AS DATE) + 181 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%''
THEN CAST(cvlr.dateofcurrentviralload AS DATE) + 181

WHEN  CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) < 1000
AND (scd.dateofviralloadsamplecollection > cvlr.dateofcurrentviralload
OR cvlr.dateofcurrentviralload IS NULL
)
AND CAST(scd.dateofviralloadsamplecollection AS DATE) + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN
CAST(scd.dateofviralloadsamplecollection AS DATE) + 91

WHEN CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) > 1000
AND ( scd.dateofviralloadsamplecollection < cvlr.dateofcurrentviralload
OR
scd.dateofviralloadsamplecollection IS NULL
)
AND CAST(cvlr.dateofcurrentviralload AS DATE) + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN
CAST(cvlr.dateofcurrentviralload AS DATE) + 91

WHEN
CAST(NULLIF(REGEXP_REPLACE(cvlr.currentviralload, ''[^0-9]'', '''', ''g''), '''') AS INTEGER) > 1000
AND (scd.dateofviralloadsamplecollection > cvlr.dateofcurrentviralload
OR cvlr.dateofcurrentviralload IS NULL)
AND CAST(scd.dateofviralloadsamplecollection AS DATE) + 91 < (SELECT date FROM expanded_radet.period WHERE is_active) AND ct.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' AND prepre.status ILIKE ''%%ACTIVE%%'' THEN
CAST(scd.dateofviralloadsamplecollection AS DATE) + 91

ELSE NULL
END
) AS dateOfVlEligibilityStatus,
(CASE WHEN cd.cd4lb IS NOT NULL THEN  cd.cd4lb
	 WHEN  ccd.cd_4 IS NOT NULL THEN CAST(ccd.cd_4 as VARCHAR)
ELSE NULL END) as lastCd4Count,
(CASE WHEN cd.dateOfCd4Lb IS NOT NULL THEN  CAST(cd.dateOfCd4Lb as DATE)
	   WHEN ccd.visit_date IS NOT NULL THEN CAST(ccd.visit_date as DATE)
ELSE NULL END) as dateOfLastCd4Count, 
INITCAP(cm.caseManager) AS caseManager,
ct.refill_period
FROM expanded_radet_client.%I bd
LEFT JOIN expanded_radet_client.%I p_lga on p_lga.personUuid11 = bd.personUuid 
AND bd.bio_ods_datim_id=p_lga.ods_datim_id
LEFT JOIN expanded_radet_client.%I pdr ON pdr.person_uuid40 = bd.personUuid
AND bd.bio_ods_datim_id=pdr.pharma_ods_datim_id
LEFT JOIN expanded_radet_client.%I c ON c.person_uuid10 = bd.personUuid
AND bd.bio_ods_datim_id=c.clin_ods_datim_id
LEFT JOIN expanded_radet_client.%I scd ON scd.personsamplecd = bd.personUuid
AND bd.bio_ods_datim_id=scd.sampd_ods_datim_id
LEFT JOIN expanded_radet_client.%I  cvlr ON cvlr.person_uuid130 = bd.personUuid
AND bd.bio_ods_datim_id=cvlr.cvl_ods_datim_id
LEFT JOIN expanded_radet_client.%I cd on cd.cd4_person_uuid = bd.personUuid
AND bd.bio_ods_datim_id=cd.lab_ods_datim_id
LEFT JOIN expanded_radet_client.%I ccd on ccd.cccd4_person_uuid = bd.personUuid
AND bd.bio_ods_datim_id=ccd.cccd4_ods_datim_id
LEFT JOIN expanded_radet_client.%I e ON e.person_uuid50 = bd.personUuid
AND bd.bio_ods_datim_id=e.eac_ods_datim_id
LEFT JOIN expanded_radet_client.%I b ON b.person_uuid60 = bd.personUuid
AND bd.bio_ods_datim_id=b.biome_ods_datim_id
LEFT JOIN expanded_radet_client.%I  ca ON ca.person_uuid70 = bd.personUuid
AND bd.bio_ods_datim_id=ca.creg_ods_datim_id
LEFT JOIN expanded_radet_client.%I ipt ON ipt.personUuid80 = bd.personUuid
AND bd.bio_ods_datim_id=ipt.cte_ipt_ods_datim_id
LEFT JOIN expanded_radet_client.%I iptN ON iptN.person_uuid = bd.personUuid
AND bd.bio_ods_datim_id=iptN.ods_datim_id
LEFT JOIN expanded_radet_client.%I cc ON cc.person_uuid90 = bd.personUuid
AND bd.bio_ods_datim_id=cc.cerv_ods_datim_id
LEFT JOIN expanded_radet_client.%I ov ON ov.personUuid100 = bd.personUuid
AND bd.bio_ods_datim_id=ov.ovc_ods_datim_id
LEFT JOIN expanded_radet_client.%I ct ON ct.cuPersonUuid = bd.personUuid
AND bd.bio_ods_datim_id=ct.cus_ods_datim_id
LEFT JOIN expanded_radet_client.%I pre ON pre.prePersonUuid = bd.personUuid
AND bd.bio_ods_datim_id = pre.pre_ods_datim_id
LEFT JOIN expanded_radet_client.%I prepre ON prepre.preprePersonUuid = bd.personUuid
and bd.bio_ods_datim_id = prepre.prepre_ods_datim_id
LEFT JOIN expanded_radet_client.%I nvd ON nvd.nvl_person_uuid = bd.personUuid
AND bd.bio_ods_datim_id=nvd.nvl_ods_datim_id
LEFT JOIN expanded_radet_client.%I tbSample ON tbSample.personTbSample = bd.personUuid
AND bd.bio_ods_datim_id=tbSample.tbsamp_ods_datim_id
LEFT JOIN expanded_radet_client.%I tbTment ON tbTment.tbTreatmentPersonUuid = bd.personUuid
AND bd.bio_ods_datim_id=tbTment.tbtreat_ods_datim_id
LEFT JOIN expanded_radet_client.%I tbTmentNew ON tbTmentNew.person_uuid_tbnew = bd.personUuid
AND bd.bio_ods_datim_id=tbTmentNew.ods_datim_id_tbnew
LEFT JOIN expanded_radet_client.%I tbResult ON tbResult.personTbResult = bd.personUuid
AND bd.bio_ods_datim_id=tbResult.ods_datim_id
LEFT JOIN expanded_radet_client.%I crypt on crypt.personuuid12= bd.personUuid
AND bd.bio_ods_datim_id=crypt.crypt_ods_datim_id
LEFT JOIN expanded_radet_client.%I tbS on tbS.person_uuid = bd.personUuid 
AND bd.bio_ods_datim_id=tbS.tbstat_ods_datim_id
LEFT JOIN expanded_radet_client.%I tbl  on tbl.personuuidtblam = bd.personUuid 
AND bd.bio_ods_datim_id=tbl.odsdatimidtblam
LEFT JOIN expanded_radet_client.%I dsd1  on dsd1.person_uuid_dsd_1 = bd.personUuid 
AND bd.bio_ods_datim_id=dsd1.dsd1_ods_datim_id
LEFT JOIN expanded_radet_client.%I dsd2  on dsd2.person_uuid_dsd_2 = bd.personUuid 
AND bd.bio_ods_datim_id=dsd2.dsd2_ods_datim_id
LEFT JOIN expanded_radet_client.%I cm on cm.caseperson= bd.personUuid
AND bd.bio_ods_datim_id=cm.case_ods_datim_id
LEFT JOIN expanded_radet_client.%I cvl on cvl.client_person_uuid = bd.personUuid 
AND bd.bio_ods_datim_id=cvl.client_ods_datim_id
LEFT JOIN expanded_radet_client.%I vaod ON vaod.person_id = bd.personUuid 
AND bd.bio_ods_datim_id=vaod.ods_datim_id
LEFT JOIN expanded_radet_client.%I iptStart ON iptStart.person_uuid_ipt_s = bd.personUuid
AND bd.bio_ods_datim_id=iptStart.ods_datim_id_ipt_s',
radetjoinedpartition,ctebiodapartition,ctepatientlgapartition,ctepharmacydetailsregimenpartition,
ctecurrentclinicalpartition,ctesamplecollectiondatepartition,ctecurrentvlresultpartition,
ctelabcd4partition,ctecarecd4partition,cteeacpartition,ctebiometricpartition,ctecurrentregimenpartition,
cteiptpartition,cteiptnewpartition,ctecervicalcancerpartition,cteovcpartition,ctecurrentstatuspartition,
ctepreviousstatuspartition,ctepreviouspreviouspartition,
ctenaivevldatapartition,ctetbsamplecollectionpartition,ctetbtreatmentpartition,ctetbtreatmentnewpartition,
ctecurrenttbresultpartition,ctecrytococalpartition,ctetbstatuspartition,ctetblampartition,ctedsd1partition,
ctedsd2partition,ctecasemanagerpartition,cteclientverificationpartition,ctevacauseofdeathpartition,
cteiptspartition);

SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,radetjoinedpartition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_radet_joined_insert(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_radet_joined_insert('tZy8wIM53xT');