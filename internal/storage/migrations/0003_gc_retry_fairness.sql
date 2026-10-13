-- Add durable attempt rotation for catalog deletion and physical-version retries.
ALTER TABLE storage_objects ADD COLUMN IF NOT EXISTS gc_attempted_at timestamptz;
CREATE INDEX IF NOT EXISTS storage_catalog_gc_fairness
  ON storage_objects(gc_attempted_at NULLS FIRST, created_at, id)
  WHERE state <> 'deleted';

ALTER TABLE storage_physical_versions ADD COLUMN IF NOT EXISTS inventory_retry_at timestamptz;
ALTER TABLE storage_physical_versions ADD COLUMN IF NOT EXISTS inventory_last_attempt_at timestamptz;
CREATE INDEX IF NOT EXISTS storage_inventory_retry_fairness
  ON storage_physical_versions(inventory_last_attempt_at NULLS FIRST, inventory_retry_at, physical_key, version)
  WHERE state='ready' AND inventory_retry_at IS NOT NULL;
