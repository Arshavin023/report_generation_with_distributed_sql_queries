-- PROCEDURE: expanded_radet_client.proc_client_verification(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_client_verification(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_client_verification(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE cteclientverification_partition text;
DECLARE hivobservation_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
cteclientverification_partition := CONCAT('cte_client_verification_',datim_id);
hivobservation_partition := CONCAT('hiv_observation_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',cteclientverification_partition);

RAISE NOTICE 'successfully truncate % table', cteclientverification_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
 SELECT * FROM (
select person_uuid client_person_uuid,ods_datim_id client_ods_datim_id, 
data->''attempt''->0->>''outcome'' AS clientVerificationOutCome,
data->''attempt''->0->>''verificationStatus'' AS clientVerificationStatus,
CAST (data->''attempt''->0->>''dateOfAttempt'' AS DATE) AS dateOfOutcome,
ROW_NUMBER() OVER ( PARTITION BY person_uuid ORDER BY CAST(data->''attempt''->0->>''dateOfAttempt'' AS DATE) DESC)
from public.%I 
where type = ''Client Verification'' AND archived = 0
AND CAST(data->''attempt''->0->>''dateOfAttempt'' AS DATE) <= %L  
AND CAST(data->''attempt''->0->>''dateOfAttempt'' AS DATE) >= ''1980-01-01'' 
) clientVerification WHERE row_number = 1
AND dateOfOutcome IS NOT NULL
',cteclientverification_partition,
hivobservation_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,cteclientverification_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_client_verification(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_client_verification('tZy8wIM53xT');