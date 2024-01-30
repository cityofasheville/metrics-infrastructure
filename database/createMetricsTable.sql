DROP TABLE IF EXISTS tbd.coa_metrics;

CREATE OR REPLACE FUNCTION metrics_update_timestamp() RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
    $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$;

CREATE TABLE tbd.coa_metrics (
	metric_id text NOT NULL,
  period_start date NOT NULL,
  period_end date NOT NULL,
  value double precision NULL,
  disaggregation_type text NULL,
  disaggregation_value text NULL,
  note text NULL,
  version integer default 0,
  updated_at datetime NOT NULL
	CONSTRAINT coa_metrics_pkey PRIMARY KEY (metric_id, period_start, period_end, version)
);

-- Permissions
ALTER TABLE bedrock.assets OWNER TO bedrock_user;
GRANT ALL ON TABLE bedrock.assets TO bedrock_user;


CREATE TRIGGER metrics_update_timestamp
BEFORE UPDATE
ON tbd.coa_metrics
FOR EACH ROW
EXECUTE PROCEDURE metrics_update_timestamp();
