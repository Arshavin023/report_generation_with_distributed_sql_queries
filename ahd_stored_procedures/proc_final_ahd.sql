-- PROCEDURE: ahd.proc_final_ahd(character varying)

-- DROP PROCEDURE IF EXISTS ahd.proc_final_ahd(character varying);

CREATE OR REPLACE PROCEDURE ahd.proc_final_ahd(
	IN ip_name character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE partition_name TEXT;
DECLARE IP_processed_ahd text;
DECLARE period_text TEXT;
DECLARE period_start DATE;
DECLARE period_end DATE;

BEGIN

SELECT TIMEOFDAY() INTO start_time;

SELECT start_date 
INTO period_start
FROM ahd.period 
WHERE is_active;

SELECT date 
INTO period_end
FROM ahd.period 
WHERE is_active;

SELECT CONCAT('ahd_',periodcode)
INTO partition_name
from ahd.period 
where is_active;

SELECT periodcode
INTO period_text
from ahd.period where is_active;

IP_processed_ahd := CONCAT('ahd_',period_text,'_',ip_name);

--Delete records in radet partition for supplied the IP in 
PERFORM dblink (
'db_link_radet',
	FORMAT('DELETE FROM public.%I
			WHERE datimid IN (SELECT datim_id FROM central_partner_mapping WHERE ip_name=%L)',
			partition_name,ip_name)
				);

PERFORM dblink(
'db_link_radet',
	FORMAT('INSERT INTO public.%I
SELECT * FROM dblink(''db_link_ods'',
''SELECT ''%L'' AS period,*,CAST(''%L'' AS DATE) AS period_start_date,CAST(''%L'' AS DATE) AS period_end_date,''%L'' ip_name
FROM ahd.ahd_joined
WHERE datimid IN (SELECT datim_id FROM central_partner_mapping WHERE ip_name=''%L'')
'') AS sm(period character varying,
    state character varying(255),
    lga character varying(255),
    facilityname character varying(255),
    datimid character varying(255),
    personuuid character varying,
    hospitalnumber character varying,
    sex character varying,
    dateofbirth date,
    age numeric,
    body_weight double precision,
    date_of_hiv_diagnosis date,
    hivenrollmentdate date,
    previous_status text,
    previous_status_date timestamp without time zone,
    current_status text,
    current_status_date timestamp without time zone,
    currentviralload character varying,
    dateofcurrentviralload date,
    viralloadindication character varying,
    dateofrepeatviralloadeacsamplecollection date,
    repeatviralloadresult character varying,
    dateofrepeatviralloadresult date,
    category text,
    visit_date date,
    staging character varying,
    diseasecondition text,
    preventingsymptoms text,
    ahdstatus text,
    lastcd4count character varying,
    dateoflastcd4count date,
    cd4_type character varying,
    lastcrytococalantigen character varying,
    dateoflastcrytococalantigen timestamp without time zone,
    lastlflam character varying,
    dateoflastlflam timestamp without time zone,
    lastserumcrag character varying,
    dateoflastserumcrag timestamp without time zone,
    lastcsfcrag character varying,
    dateoflastcsfcrag timestamp without time zone,
    treatmentdate text,
    period_start_date date,
    period_end_date date,
    ip_name character varying)
',partition_name,period_text,period_start, period_end,ip_name,ip_name)
);


UPDATE ahd.period
SET is_ahd_available = true
WHERE periodcode = period_text;
		
PERFORM dblink('db_link_radet',
	FORMAT('
		UPDATE ahd.period
		SET is_ahd_available = true
		WHERE periodcode = %L',period_text)
);

SELECT TIMEOFDAY() INTO end_time; 

PERFORM dblink('db_link_radet',
	FORMAT('
		INSERT INTO public.ahd_monitoring (table_name, start_time,end_time)
		VALUES (%L,%L,%L)',IP_processed_ahd, start_time,end_time)
);

END;
$BODY$;
ALTER PROCEDURE ahd.proc_final_ahd(character varying)
    OWNER TO lamisplus_etl;

CALL ahd.proc_final_ahd('ACE-1');
