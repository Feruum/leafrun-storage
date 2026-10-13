-- Additive migration: existing pins keep their state and become first-attempt candidates.
ALTER TABLE storage_pins ADD COLUMN IF NOT EXISTS last_reconciled_at timestamptz;
CREATE INDEX IF NOT EXISTS storage_pin_reconciliation_fairness ON storage_pins(last_reconciled_at NULLS FIRST,expires_at,id) WHERE state IN ('pending','unresolved','active');
