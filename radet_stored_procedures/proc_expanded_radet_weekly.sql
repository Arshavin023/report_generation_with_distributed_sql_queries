-- PROCEDURE: expanded_radet.proc_expanded_radet_weekly(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet.proc_expanded_radet_weekly(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet.proc_expanded_radet_weekly(
	IN ip_name character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE partition_name TEXT;
DECLARE IP_processed_radet text;
DECLARE period_text TEXT;
DECLARE period_start DATE;
DECLARE period_end DATE;

BEGIN

SELECT TIMEOFDAY() INTO start_time;

SELECT start_date 
INTO period_start
FROM expanded_radet.period 
WHERE is_active;

SELECT date 
INTO period_end
FROM expanded_radet.period 
WHERE is_active;

SELECT CONCAT('radet_copy_',periodcode)
INTO partition_name
from expanded_radet.period 
where is_active;

SELECT periodcode
INTO period_text
from expanded_radet.period where is_active;

IP_processed_radet := CONCAT('radet_',period_text,'_',ip_name);

--Delete records in radet partition for supplied the IP in 
PERFORM dblink (
'db_link_radet',
	FORMAT('DELETE FROM public.%I
			WHERE datim_id IN (SELECT datim_id FROM central_partner_mapping WHERE ip_name=%L)',
			partition_name,ip_name)
				);

PERFORM dblink(
'db_link_radet',
	FORMAT('INSERT INTO public.%I(
				period, uniquepersonuuid, datim_id, hospitalnumber, uniqueid, age, gender, dateofbirth,
				facilityname, lga, state, datimid, targetgroup, enrollmentsetting, artstartdate, 
				regimenatstart, dateofregistration, dateofenrollment, ovcuniqueid, householduniqueno,
				careentry, regimenlineatstart, ndrpatientidentifier, dateofviralloadsamplecollection, 
				dateofcurrentviralloadsample, vlfacility, vlarchived, viralloadindication, 
				currentviralload, dateofcurrentviralload, dsdmodel, dateofstartofcurrentartregimen, 
				lastpickupdate, currentartregimen, currentregimenline, nextpickupdate,
				monthsofarvrefill, datebiometricsenrolled, numberoffingerscaptured,
				dateofcommencementofeac, numberofeacsessioncompleted, dateoflasteacsessioncompleted, 
				dateofextendeaccompletion, dateofrepeatviralloadresult, repeatviralloadresult, 
				dateofiptstart, iptcompletiondate, iptcompletionstatus, ipttype, 
				dateofcervicalcancerscreening, treatmentmethoddate, cervicalcancerscreeningtype, 
				cervicalcancerscreeningmethod, cervicalcancertreatmentscreened, 
				resultofcervicalcancerscreening, ovcnumber, householdnumber, tbtreatmenttype, 
				tbtreatmentstartdate, tbtreatmentoutcome, tbcompletiondate, tbtreatmentpersonuuid,
				dateoftbsamplecollection, persontbsample, persontbresult, dateoftbdiagnosticresultreceived, 
				date_result_reported, tbdiagnosticresult, tbdiagnostictesttype, dateoftbscreened, tbstatus, 
				dateoflasttblam, tblamresult, causeofdeath, vacauseofdeath, previousstatus, 
				previousstatusdate, currentstatus, currentstatusdate, vleligibilitystatus, dateofvleligibilitystatus, 
				lastcd4count, dateoflastcd4count, dateoflastcrytococalantigen, lastcrytococalantigen, 
				casemanager, clientverificationstatus, currentweight, pregnancystatus, dateofcurrentartstatus,
				clientverificationoutcome, modeldevolvedto, dateofdevolvement, currentdsdmodel, 
				dateofcurrentdsd, datereturntosite, dateofrepeatviralloadeacsamplecollection, 
				tbscreeningtype, tbstatusoutcome, currentclinicalstage, datebiometricsrecaptured, 
				numberoffingersrecaptured, period_start_date, period_end_date
				)
SELECT * FROM dblink(''db_link_ods'',
''SELECT ''%L'' AS period,uniquepersonuuid, bio_ods_datim_id AS datim_id, hospitalnumber, uniqueid,
age,gender,CAST(dateofbirth AS DATE) dateofbirth,facilityname,lga,state,bio_ods_datim_id datimid,
targetgroup,enrollmentsetting,CAST(artstartdate AS DATE) artstartdate,regimenatstart,
CAST(dateofregistration AS DATE) dateofregistration,CAST(dateofenrollment AS DATE) dateofenrollment,
ovcuniqueid,householduniqueno,careentry,regimenlineatstart,ndrpatientidentifier,
CAST(dateofviralloadsamplecollection AS DATE) dateofviralloadsamplecollection,  
CAST(dateofcurrentviralloadsample AS DATE) dateofcurrentviralloadsample, 
vlfacility,vlarchived,viralloadindication,currentviralload,CAST(dateofcurrentviralload AS DATE) dateofcurrentviralload,
dsdmodel,CAST(dateofStartofCurrentARTRegimen AS DATE) AS dateofstartofcurrentartregimen,
CAST(lastpickupdate AS DATE) AS lastpickupdate,currentartregimen,currentregimenline,
CAST(nextpickupdate AS DATE) nextpickupdate,CAST(CAST(refill_period AS INTEGER) / 30.0 AS DECIMAL(10, 1)) AS monthsofarvrefill,
CAST(datebiometricsenrolled AS DATE) datebiometricsenrolled,numberoffingerscaptured,
CAST(dateofcommencementofeac AS DATE) dateofcommencementofeac,numberofeacsessioncompleted,
CAST(dateoflasteacsessioncompleted AS DATE) dateoflasteacsessioncompleted,
CAST(dateofextendeaccompletion AS DATE) dateofextendeaccompletion,
CAST(dateofrepeatviralloadresult AS DATE) dateofrepeatviralloadresult,
repeatviralloadresult,CAST(dateofiptstart AS DATE) dateofiptstart,
CAST(iptcompletiondate AS DATE) iptcompletiondate,iptcompletionstatus,
ipttype,
CAST(dateofcervicalcancerscreening AS DATE) dateofcervicalcancerscreening,
CASE WHEN trim(treatmentmethoddate) <> '''''''' AND treatmentmethoddate ~ ''''^\d+$''''
THEN CAST(treatmentmethoddate as date) ELSE NULL END AS treatmentmethoddate,
cervicalcancerscreeningtype,cervicalcancerscreeningmethod,cervicalcancertreatmentscreened,
resultofcervicalcancerscreening,ovcnumber,householdnumber,
tbtreatementtype AS tbtreatmenttype,CAST(tbtreatmentstartdate AS DATE) tbtreatmentstartdate,
tbtreatmentoutcome,CAST(tbcompletiondate AS DATE) tbcompletiondate,
tbtreatmentpersonuuid,dateoftbsamplecollection,uniquepersonuuid persontbsample,
persontbresult,CAST(dateofTbDiagnosticResultReceived AS DATE) dateofTbDiagnosticResultReceived,
CAST(dateofTbDiagnosticResultReceived AS DATE) AS date_result_reported,
tbdiagnosticresult,tbdiagnostictesttype,CAST(dateoftbscreened AS DATE) dateoftbscreened,
tbstatus,CAST(dateoflasttblam AS DATE) dateoflasttblam,tblamresult,
causeofdeath,vacauseofdeath,previousstatus,previousstatusdate,currentstatus,
CAST(currentstatusdate AS DATE) currentstatusdate,vleligibilitystatus,
CAST(dateofvleligibilitystatus AS DATE) dateofvleligibilitystatus,
lastcd4count,CAST(dateoflastcd4count AS DATE) dateoflastcd4count,
CAST(dateoflastcrytococalantigen AS DATE) dateoflastcrytococalantigen,
lastcrytococalantigen,casemanager,clientverificationstatus,currentweight,
pregnancyStatus,currentStatusDate AS dateofcurrentartstatus,clientverificationoutcome,
modeldevolvedto,dateofdevolvement,currentdsdmodel,dateofcurrentdsd,datereturntosite,
dateofrepeatviralloadeacsamplecollection,tbscreeningtype,
'''''''' tbStatusOutcome,currentclinicalstage,datebiometricsrecaptured,numberoffingersrecaptured,
CAST(''%L'' AS DATE) AS period_start_date,CAST(''%L'' AS DATE) AS period_end_date
FROM expanded_radet.obt_radet
WHERE bio_ods_datim_id IN (SELECT datim_id FROM central_partner_mapping WHERE ip_name=''%L'')
'') AS sm(period text,
    uniquepersonuuid character varying,
    datim_id character varying(255),
    hospitalnumber character varying,
    uniqueid character varying,
    age numeric,
    gender text,
    dateofbirth date,
    facilityname character varying,
    lga character varying,
    state character varying,
    datimid character varying,
    targetgroup character varying,
    enrollmentsetting character varying,
    artstartdate date,
    regimenatstart character varying,
    dateofregistration date,
    dateofenrollment date,
    ovcuniqueid character varying,
    householduniqueno character varying,
    careentry character varying,
    regimenlineatstart character varying,
    ndrpatientidentifier text,
    dateofviralloadsamplecollection date,
    dateofcurrentviralloadsample date,
    vlfacility integer,
    vlarchived integer,
    viralloadindication character varying,
    currentviralload character varying,
    dateofcurrentviralload date,
    dsdmodel character varying,
    dateofstartofcurrentartregimen date,
    lastpickupdate date,
    currentartregimen character varying,
    currentregimenline character varying,
    nextpickupdate date,
    monthsofarvrefill numeric(10,1),
    datebiometricsenrolled date,
    numberoffingerscaptured bigint,
    dateofcommencementofeac date,
    numberofeacsessioncompleted bigint,
    dateoflasteacsessioncompleted date,
    dateofextendeaccompletion date,
    dateofrepeatviralloadresult date,
    repeatviralloadresult character varying,
    dateofiptstart date,
    iptcompletiondate date,
    iptcompletionstatus text,
    ipttype character varying,
    dateofcervicalcancerscreening date,
    treatmentmethoddate date,
    cervicalcancerscreeningtype character varying,
    cervicalcancerscreeningmethod character varying,
    cervicalcancertreatmentscreened character varying,
    resultofcervicalcancerscreening character varying,
    ovcnumber character varying,
    householdnumber character varying,
    tbtreatmenttype text,
    tbtreatmentstartdate date,
    tbtreatmentoutcome text,
    tbcompletiondate date,
    tbtreatmentpersonuuid character varying,
    dateoftbsamplecollection date,
    persontbsample character varying,
    persontbresult character varying,
    dateoftbdiagnosticresultreceived date,
    date_result_reported date,
    tbdiagnosticresult text,
    tbdiagnostictesttype text,
    dateoftbscreened date,
    tbstatus text,
    dateoflasttblam date,
    tblamresult character varying,
    causeofdeath character varying,
    vacauseofdeath character varying,
    previousstatus text,
    previousstatusdate date,
    currentstatus text,
    currentstatusdate date,
    vleligibilitystatus boolean,
    dateofvleligibilitystatus date,
    lastcd4count character varying,
    dateoflastcd4count date,
    dateoflastcrytococalantigen date,
    lastcrytococalantigen character varying,
    casemanager text,
    clientverificationstatus text,
    currentweight double precision,
    pregnancystatus character varying,
    dateofcurrentartstatus date,
    clientverificationoutcome text,
    modeldevolvedto character varying,
    dateofdevolvement date,
    currentdsdmodel character varying,
    dateofcurrentdsd date,
    datereturntosite date,
    dateofrepeatviralloadeacsamplecollection date,
    tbscreeningtype text,
    tbstatusoutcome text,
    currentclinicalstage character varying,
    datebiometricsrecaptured date,
    numberoffingersrecaptured bigint,
    period_start_date date,
    period_end_date date)
',partition_name,period_text,period_start, period_end,ip_name)
);

PERFORM dblink('db_link_radet',
	FORMAT('
		UPDATE public.%I t1 
		SET ip_name=t2.ip_name, facilityname=t2.facility_name, 
		state=t2.facility_state,lga=t2.facility_lga 
		FROM public.central_partner_mapping t2 
		WHERE t2.datim_id=t1.datim_id 
		AND t1.ip_name IS NULL
		AND t2.ip_name = %L',partition_name,ip_name)
);

PERFORM dblink('db_link_radet',
	FORMAT('
		UPDATE public.%I
		SET hospitalnumber = ''******''
		WHERE hospitalnumber != ''******''
		AND ip_name=%L',partition_name,ip_name)
);

SELECT TIMEOFDAY() INTO end_time; 

PERFORM dblink('db_link_radet',
	FORMAT('
		INSERT INTO expanded_radet.expanded_radet_monitoring (table_name, start_time,end_time)
		VALUES (%L,%L,%L)',IP_processed_radet, start_time,end_time)
);

END;
$BODY$;
ALTER PROCEDURE expanded_radet.proc_expanded_radet_weekly(character varying)
    OWNER TO lamisplus_etl;

-- CALL expanded_radet.proc_expanded_radet_weekly('ACE-1');

-- SELECT dblink_connect('db_link_radet')