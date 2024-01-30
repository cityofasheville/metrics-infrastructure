DROP TABLE IF EXISTS metric.coa_metrics;
DROP FUNCTION IF EXISTS metric.metrics_update_timestamp;

CREATE TABLE metric.coa_metrics (
	metric_id text NOT NULL,
  period_start date NOT NULL,
  period_end date NOT NULL,
  value double precision NULL,
  disaggregation_type text NULL,
  disaggregation_value text NULL,
  note text NULL,
  version integer default 0,
  updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT coa_metrics_pkey PRIMARY KEY (metric_id, period_start, period_end, version)
);

-- Permissions
--ALTER TABLE bedrock.assets OWNER TO bedrock_user;
--GRANT ALL ON TABLE bedrock.assets TO bedrock_user;

CREATE OR REPLACE FUNCTION metric.metrics_update_timestamp() RETURNS TRIGGER
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
ON metric.coa_metrics
FOR EACH ROW
EXECUTE PROCEDURE metric.metrics_update_timestamp();

insert into metric.coa_metrics (metric_id, period_start, period_end)
  values ('abc', '2024-01-30', '2024-02-29');
