-- We no longer have a spot requests table, drop it and its
-- touched column trigger
DROP TRIGGER IF EXISTS update_spotrequests_touched ON spotrequests;
DROP TRIGGER IF EXISTS update_spotrequests_touched ON spotrequests;
DROP TABLE IF EXISTS spotrequests;
DROP TABLE IF EXISTS ebsusage;

-- We no longer track the SRID, drop it
ALTER TABLE instances DROP IF EXISTS srid CASCADE;

-- Fix instances table cases
ALTER TABLE instances RENAME COLUMN workertype to "workerType";
ALTER TABLE instances RENAME COLUMN instancetype to "instanceType";
ALTER TABLE instances RENAME COLUMN imageid to "imageId";
ALTER TABLE instances RENAME COLUMN lastevent to "lastEvent";

-- Fix amiusage table cases
ALTER TABLE amiusage RENAME COLUMN lastused to "lastUsed";

-- termination reasons
CREATE TABLE IF NOT EXISTS terminations (
  id VARCHAR(128) NOT NULL, -- opaque ID per Amazon
  "workerType" VARCHAR(128) NOT NULL, -- taskcluster worker type
  region VARCHAR(128) NOT NULL, -- ec2 region
  az VARCHAR(128) NOT NULL, -- availability zone
  "instanceType" VARCHAR(128) NOT NULL, -- ec2 instance type
  "imageId" VARCHAR(128) NOT NULL, -- AMI/ImageId value
  code VARCHAR(128), -- the State Reason's code
  reason VARCHAR(128), -- the State Reason's string message
  launched TIMESTAMPTZ NOT NULL, -- Time instance launched
  terminated TIMESTAMPTZ, -- Time the instance shut down
  "lastEvent" TIMESTAMPTZ NOT NULL, -- Time that the last event happened in the api. Used
                                    -- to ensure that we have correct ordering of cloud watch
                                    -- events
  touched TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(id, region)
);
-- Automatically keep instances touched parameter up to date
CREATE TRIGGER update_terminations_touched
BEFORE UPDATE ON terminations
FOR EACH ROW EXECUTE PROCEDURE update_touched();


