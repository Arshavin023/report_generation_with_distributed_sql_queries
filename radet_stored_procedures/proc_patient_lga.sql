-- PROCEDURE: expanded_radet_client.proc_patient_lga(character varying)

-- DROP PROCEDURE IF EXISTS expanded_radet_client.proc_patient_lga(character varying);

CREATE OR REPLACE PROCEDURE expanded_radet_client.proc_patient_lga(
	IN datim_id character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    ctepatientlga_partition text;
    patientperson_partition text;
    baseorganisationunit_partition text;
	radetmonitoringpartition text;

BEGIN
    -- Construct dynamic table references
    ctepatientlga_partition := CONCAT('cte_patient_lga_', datim_id);
    patientperson_partition := CONCAT('patient_person_', datim_id);
    baseorganisationunit_partition := CONCAT('base_organisation_unit_', datim_id);
	radetmonitoringpartition := CONCAT('radet_monitoring_',datim_id);

    -- Log start time
    SELECT TIMEOFDAY() INTO start_time;

    -- Delete existing rows for the specific datim_id
    EXECUTE FORMAT('TRUNCATE expanded_radet_client.%I',ctepatientlga_partition);

    RAISE NOTICE 'Successfully truncated table %', ctepatientlga_partition;

    -- Perform the insertion
    EXECUTE FORMAT('
        INSERT INTO expanded_radet_client.%I
        SELECT DISTINCT ON (dt.person_uuid) 
            dt.person_uuid AS personuuid11,
            dt.ods_datim_id,
            facility_state.name AS stateofresidence,
            facility_lga.name AS lgaofresidence
        FROM (
            SELECT 
                pp.uuid AS person_uuid,
                pp.ods_datim_id,
                (jsonb_array_elements((pp.address::jsonb->>''value'')::jsonb->''address'')->>''district'') AS addr_lga,
                (jsonb_array_elements((pp.address::jsonb->>''value'')::jsonb->''address'')->>''stateId'') AS addr_state
            FROM public.%I pp
        ) dt
        LEFT JOIN public.%I facility_lga 
            ON facility_lga.id = CAST(
                CASE 
                    WHEN (addr_lga ~ ''^[0-9.]+$'') THEN dt.addr_lga 
                    ELSE NULL 
                END AS INTEGER)
        LEFT JOIN public.%I facility_state 
            ON facility_state.id = CAST(
                CASE 
                    WHEN (addr_state ~ ''^[0-9.]+$'') THEN dt.addr_state 
                    ELSE NULL 
                END AS INTEGER)',
		ctepatientlga_partition,
        patientperson_partition,
        baseorganisationunit_partition,
        baseorganisationunit_partition
    );

    -- -- Create index if it doesn't already exist
    -- CREATE INDEX IF NOT EXISTS idx_personuuiddatimid_cte_patient_lga
    -- ON expanded_radet_client.cte_patient_lga 
    -- USING btree (personuuid11, ods_datim_id);

    -- Log end time
    SELECT TIMEOFDAY() INTO end_time;

    -- Insert monitoring record
    EXECUTE FORMAT('INSERT INTO expanded_radet_client.%I
	(table_name, start_time,end_time,datim_id) 
	VALUES (%L,%L, %L, %L)',
	radetmonitoringpartition,ctepatientlga_partition,start_time,end_time,datim_id);

END;
$BODY$;
ALTER PROCEDURE expanded_radet_client.proc_patient_lga(character varying)
    OWNER TO lamisplus_etl;
CALL expanded_radet_client.proc_patient_lga('tZy8wIM53xT');