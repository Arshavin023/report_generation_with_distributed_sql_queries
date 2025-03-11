-- PROCEDURE: expanded_radet.proc_update_expanded_radet_period_table()

-- DROP PROCEDURE IF EXISTS expanded_radet.proc_update_expanded_radet_period_table();

CREATE OR REPLACE PROCEDURE expanded_radet.proc_update_expanded_radet_period_table(
IN periodcode character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
BEGIN
SELECT TIMEOFDAY() INTO start_time;
			  
PERFORM dblink('db_link_radet',
      format('update expanded_radet.period 
              set is_active = false
			 '));

PERFORM dblink('db_link_radet',
      format('update expanded_radet.period 
              set is_active = true, is_radet_available=false
              where periodcode in (%L)
			 ',periodcode
			 ));
			 
update expanded_radet.period 
SET is_active = false;

EXECUTE FORMAT('update expanded_radet.period 
SET is_active = true, is_radet_available=false
WHERE periodcode in (%L)
			',periodcode);

SELECT TIMEOFDAY() INTO end_time;
INSERT INTO expanded_radet.expanded_radet_monitoring (table_name, start_time,end_time) 
VALUES ('period', start_time,end_time);
END
$BODY$;
ALTER PROCEDURE expanded_radet.proc_update_expanded_radet_period_table(character varying)
    OWNER TO lamisplus_etl;
