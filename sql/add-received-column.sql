BEGIN;

ALTER TABLE cloudwatchlog ADD COLUMN received TIMESTAMPTZ;
UPDATE cloudwatchlog SET received = TIMESTAMPTZ '1970-1-1 1:0:0';
ALTER TABLE cloudwatchlog ALTER COLUMN received SET NOT NULL;

COMMIT;

