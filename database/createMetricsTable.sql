DROP TABLE IF EXISTS internal.coa_metrics;
DROP FUNCTION IF EXISTS internal.metrics_update_timestamp;

CREATE TABLE internal.coa_metrics (
	metric_id text NOT NULL,
  period_start date NOT NULL,
  period_end date NOT NULL,
  value double precision NULL,
  disaggregation_type text NULL,
  disaggregation_value text NULL,
  note text NULL,
  version integer default 0,
  updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT coa_metrics_pkey PRIMARY KEY (metric_id, period_start, period_end, disaggregation_type, disaggregation_value, version)
);

CREATE OR REPLACE FUNCTION internal.metrics_update_timestamp() RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
    $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$;

CREATE TRIGGER metrics_update_timestamp
BEFORE UPDATE
ON internal.coa_metrics
FOR EACH ROW
EXECUTE PROCEDURE internal.metrics_update_timestamp();
