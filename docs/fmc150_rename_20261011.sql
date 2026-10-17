-- FMC150 (862129089514594): rename fields that were stored under FMC650 AVL ID names.
-- Run AFTER restarting teltonika2mqtt, so no new mislabeled rows arrive afterwards.
BEGIN;
CREATE TABLE IF NOT EXISTS fmc150_field_rename_backup_20261011 AS
  SELECT id, time, field AS old_field FROM measurements WHERE false;
INSERT INTO fmc150_field_rename_backup_20261011
  SELECT id, time, field FROM measurements
  WHERE device='862129089514594' AND measurement='data'
    AND field IN ('engine_load_percent','io_81','io_89','io_107');
UPDATE measurements SET field = CASE field
    WHEN 'engine_load_percent' THEN 'engine_rpm'
    WHEN 'io_81' THEN 'can_speed_kmh'
    WHEN 'io_89' THEN 'fuel_level_percent'
    WHEN 'io_107' THEN 'fuel_consumed_counted' END
  WHERE device='862129089514594' AND measurement='data'
    AND field IN ('engine_load_percent','io_81','io_89','io_107');
SELECT field, count(*) FROM measurements WHERE device='862129089514594' AND measurement='data'
  AND field IN ('engine_load_percent','io_81','io_89','io_107','engine_rpm','can_speed_kmh','fuel_level_percent','fuel_consumed_counted')
  GROUP BY 1;
COMMIT;
