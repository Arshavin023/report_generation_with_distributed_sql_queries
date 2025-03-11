-- PROCEDURE: expanded_radet_client.proc_cervical_cancer(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_cervical_cancer(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_cervical_cancer(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctecervicalcancer_partition text;
DECLARE hivobservation_partition text;
DECLARE baseappcodeset_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;
ctecervicalcancer_partition := CONCAT('cte_cervical_cancer_',datim_id);
hivobservation_partition := CONCAT('hiv_observation_',datim_id);
baseappcodeset_partition := CONCAT('base_application_codeset_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctecervicalcancer_partition);

RAISE NOTICE 'successfully truncate % table', ctecervicalcancer_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
select * from (select  ho.person_uuid AS person_uuid90, ho.ods_datim_id cerv_ods_datim_id,
ho.date_of_observation AS dateOfCervicalCancerScreening, 
ho.data ->> ''screenTreatmentMethodDate'' AS treatmentMethodDate,cc_type.display AS cervicalCancerScreeningType, 
cc_method.display AS cervicalCancerScreeningMethod, cc_trtm.display AS cervicalCancerTreatmentScreened, 
cc_result.display AS resultOfCervicalCancerScreening, 
ROW_NUMBER() OVER (PARTITION BY ho.person_uuid ORDER BY ho.date_of_observation DESC) AS row 
from public.%I ho 
LEFT JOIN public.%I  cc_type ON cc_type.code = CAST(ho.data ->> ''screenType'' AS VARCHAR) 
LEFT JOIN public.%I cc_method ON cc_method.code = CAST(ho.data ->> ''screenMethod'' AS VARCHAR) 
LEFT JOIN public.%I cc_result ON cc_result.code = CAST(ho.data ->> ''screeningResult'' AS VARCHAR) 
LEFT JOIN public.%I cc_trtm ON cc_trtm.code = CAST(ho.data ->> ''screenTreatment'' AS VARCHAR) 
where ho.archived = 0 and type = ''Cervical cancer'') as cc where row = 1
',ctecervicalcancer_partition,hivobservation_partition,
baseappcodeset_partition,baseappcodeset_partition,
baseappcodeset_partition,baseappcodeset_partition,period_end_date);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctecervicalcancer_partition,start_time,end_time,datim_id);
END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_cervical_cancer(character varying)
    OWNER TO lamisplus_etl;

CALL expanded_radet_client.proc_cervical_cancer('tZy8wIM53xT');