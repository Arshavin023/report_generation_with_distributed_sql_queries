-- PROCEDURE: expanded_radet_client.proc_naive_vl_data(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_naive_vl_data(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_naive_vl_data(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE ctenaivevldata_partition text;
DECLARE patientperson_partition text;
DECLARE hivartpharmacy_partition text;
DECLARE hivartpharmacyregimens_partition text;
DECLARE hivregimen_partition text;
DECLARE hivregimentype_partition text;
DECLARE hivregimenresolver_partition text;
DECLARE laboratorysample_partition text;
DECLARE laboratorytest_partition text;
DECLARE period_end_date DATE;
DECLARE radetmonitoringpartition text;

BEGIN
SELECT date 
INTO period_end_date
FROM expanded_radet.period WHERE is_active;

ctenaivevldata_partition := CONCAT('cte_naive_vl_data_',datim_id);
patientperson_partition := CONCAT('patient_person_',datim_id);
hivartpharmacy_partition := CONCAT('hiv_art_pharmacy_',datim_id);
-- revert back to this later
hivartpharmacyregimens_partition := CONCAT('hiv_art_pharmacy_regimens_',datim_id);
-- hivartpharmacyregimens_partition := CONCAT('hapr_20240930_',datim_id);
hivregimen_partition := CONCAT('hiv_regimen_',datim_id);
hivregimentype_partition := CONCAT('hiv_regimen_type_',datim_id);
hivregimenresolver_partition := CONCAT('hiv_regimen_resolver_',datim_id);
laboratorysample_partition := CONCAT('laboratory_sample_',datim_id);
laboratorytest_partition := CONCAT('laboratory_test_',datim_id);
radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

SELECT TIMEOFDAY() INTO start_time;

EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctenaivevldata_partition);

RAISE NOTICE 'successfully truncate % table', ctenaivevldata_partition;

EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
SELECT pp.uuid AS nvl_person_uuid,pp.ods_datim_id nvl_ods_datim_id,
EXTRACT(YEAR FROM AGE(%L, pp.date_of_birth)) as age, ph.visit_date, ph.regimen
FROM public.%I pp
INNER JOIN (SELECT DISTINCT * FROM (SELECT pharm.*,
ROW_NUMBER() OVER (PARTITION BY pharm.person_uuid ORDER BY pharm.visit_date DESC)
FROM (SELECT DISTINCT * FROM  %I hap
INNER JOIN public.%I hapr
INNER JOIN public.%I hr ON hr.id=hapr.regimens_id
INNER JOIN public.%I hrt ON hrt.id=hr.regimen_type_id
INNER JOIN public.%I hrr ON hrr.regimensys=hr.description
ON hapr.art_pharmacy_id=hap.id
WHERE hap.archived=0 AND hrt.id IN (1,2,3,4,14, 16)) pharm
)ph WHERE ph.row_number=1
)ph ON ph.person_uuid=pp.uuid
WHERE pp.uuid NOT IN (
SELECT patient_uuid FROM (
SELECT COUNT(ls.patient_uuid), ls.patient_uuid 
FROM public.%I ls
INNER JOIN public.%I lt ON lt.id=ls.test_id AND lt.lab_test_id=16
WHERE ls.archived=0 
GROUP BY ls.patient_uuid
  )t )',ctenaivevldata_partition,period_end_date,patientperson_partition,
  hivartpharmacy_partition,hivartpharmacyregimens_partition,
hivregimen_partition,hivregimentype_partition,hivregimenresolver_partition,
laboratorysample_partition,laboratorytest_partition);
	
SELECT TIMEOFDAY() INTO end_time;
EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
(table_name, start_time,end_time,datim_id) 
VALUES (%L,%L, %L, %L)',
radetmonitoringpartition,ctenaivevldata_partition,start_time,end_time,datim_id);

END
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_naive_vl_data(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_naive_vl_data('tZy8wIM53xT');