-- PROCEDURE: ahd.proc_lastcd4(character varying)

-- DROP PROCEDURE IF EXISTS ahd.proc_lastcd4(character varying);

CREATE OR REPLACE PROCEDURE ahd.proc_lastcd4(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE patientperson_partition text;
DECLARE ctecareCardCD4_partition text;
DECLARE ctelabcd4_partition text;
DECLARE ctelastcd4_partition text;
DECLARE period_end_date DATE;
DECLARE ahdmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM ahd.period WHERE is_active;

patientperson_partition := CONCAT('patient_person_',datim_id);
ctelabcd4_partition := CONCAT('cte_labcd4_',datim_id);
ctecareCardCD4_partition := CONCAT('cte_carecardcd4_',datim_id);
ctelastcd4_partition := CONCAT('cte_lastcd4_',datim_id);
ahdmonitoringpartition := CONCAT('ahd_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE ahd.%I', ctelastcd4_partition);

RAISE NOTICE 'successfully truncate % table', ctelastcd4_partition;

EXECUTE FORMAT('INSERT INTO ahd.%I
SELECT p.uuid AS person_uuid,COALESCE(cd.cd4Lb,ccd.cd_4) AS lastCd4Count,
COALESCE(CAST(cd.dateOfCD4Lb AS DATE),
CAST(ccd.visit_date AS DATE)) AS dateOfLastCd4Count
FROM public.%I p
LEFT JOIN ahd.%I cd ON cd.cd4_person_uuid = p.uuid
LEFT JOIN ahd.%I ccd ON ccd.cccd4_person_uuid = p.uuid
',ctelastcd4_partition,patientperson_partition,ctelabcd4_partition,
ctecareCardCD4_partition);

SELECT TIMEOFDAY() INTO end_time;

EXECUTE FORMAT('INSERT INTO ahd.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
ahdmonitoringpartition,ctelastcd4_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE ahd.proc_lastcd4(character varying)
    OWNER TO lamisplus_etl;

-- CALL ahd.proc_carecardcd4('A1xxdELs2fm');

-- SELECT * FROM ahd.ahd_monitoring;