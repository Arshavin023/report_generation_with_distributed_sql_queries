-- PROCEDURE: expanded_radet_client.proc_biometric(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_biometric(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_biometric(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctebiometric_partition text;
DECLARE biometric_partition text;
DECLARE hivenrollment_partition text;
DECLARE hivstatustracker_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;

ctebiometric_partition := CONCAT('cte_biometric_',datim_id);
hivenrollment_partition := CONCAT('hiv_enrollment_',datim_id);
biometric_partition := CONCAT('biometric_',datim_id);
hivstatustracker_partition := CONCAT('hiv_status_tracker_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT ('TRUNCATE expanded_radet_client.%I',ctebiometric_partition);

RAISE NOTICE 'successfully truncate % table', ctebiometric_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT 
DISTINCT ON (he.person_uuid,he.ods_datim_id) he.person_uuid AS person_uuid60, 
he.ods_datim_id biome_ods_datim_id,biometric_count.enrollment_date AS dateBiometricsEnrolled, 
biometric_count.count AS numberOfFingersCaptured,
recapture_count.recapture_date AS dateBiometricsRecaptured,
recapture_count.count AS numberOfFingersRecaptured,bst.biometric_status AS biometricStatus, 
bst.status_date
FROM public.%I he 
LEFT JOIN (SELECT b.person_uuid, 
CASE WHEN COUNT(b.person_uuid) > 10 THEN 10 ELSE COUNT(b.person_uuid) END, 
MAX(enrollment_date) enrollment_date 
FROM public.%I b 
WHERE archived = 0 AND (recapture = 0 or recapture is null) 
GROUP BY b.person_uuid) biometric_count ON biometric_count.person_uuid = he.person_uuid 
LEFT JOIN (SELECT b.person_uuid, max_capture.max_capture_date AS recapture_date, b.recapture,
CASE WHEN COUNT(b.person_uuid) > 10 THEN 10 ELSE COUNT(b.person_uuid) END
FROM public.%I b
LEFT JOIN (select person_uuid,max(enrollment_date) max_capture_date 
from public.%I group by person_uuid) max_capture
ON b.person_uuid=max_capture.person_uuid
where b.enrollment_date=max_capture.max_capture_date 
AND b.archived=0 AND b.recapture !=0 and b.recapture is NOT null 
group by 1,2,3
order by b.person_uuid
) recapture_count ON recapture_count.person_uuid = he.person_uuid 
LEFT JOIN (SELECT DISTINCT ON (person_id,ods_datim_id) person_id, biometric_status,
MAX(status_date) OVER (PARTITION BY person_id ORDER BY status_date DESC) AS status_date 
FROM public.%I 
WHERE archived=0) bst ON bst.person_id = he.person_uuid 
WHERE he.archived = 0
',ctebiometric_partition,hivenrollment_partition,biometric_partition,biometric_partition,
biometric_partition,hivstatustracker_partition);
	
SELECT TIMEOFDAY() INTO end_time;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctebiometric_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_biometric(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_biometric('tZy8wIM53xT');