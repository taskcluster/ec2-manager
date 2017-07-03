BEGIN;

ALTER TABLE instances ADD COLUMN az varchar(128);
ALTER TABLE instances ADD COLUMN imageid varchar(128);
ALTER TABLE instances ADD COLUMN launched TIMESTAMPTZ;

UPDATE instances SET az = 'undefined', imageid = 'undefined', launched = TIMESTAMPTZ '1970-1-1 1:0:0';

ALTER TABLE instances ALTER COLUMN az SET NOT NULL;
ALTER TABLE instances ALTER COLUMN imageid SET NOT NULL;
ALTER TABLE instances ALTER COLUMN launched SET NOT NULL;

ALTER TABLE spotrequests ADD COLUMN az varchar(128);
ALTER TABLE spotrequests ADD COLUMN imageid varchar(128);
ALTER TABLE spotrequests ADD COLUMN created TIMESTAMPTZ;

UPDATE spotrequests SET az = 'undefined', imageid = 'undefined', created = TIMESTAMPTZ '1970-1-1 1:0:0';

ALTER TABLE spotrequests ALTER COLUMN az SET NOT NULL;
ALTER TABLE spotrequests ALTER COLUMN imageid SET NOT NULL;
ALTER TABLE spotrequests ALTER COLUMN created SET NOT NULL;


COMMIT;

