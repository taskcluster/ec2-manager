-- We no longer have a spot requests table, drop it and its
-- touched column trigger
DROP TRIGGER IF EXISTS update_spotrequests_touched ON spotrequests;
DROP TABLE IF EXISTS spotrequests;

-- We no longer track the SRID, drop it
ALTER TABLE instances DROP IF EXISTS srid CASCADE;

-- Fix instances table cases
ALTER TABLE instances RENAME COLUMN workertype to "workerType";
ALTER TABLE instances RENAME COLUMN instancetype to "instanceType";
ALTER TABLE instances RENAME COLUMN imageid to "imageId";
ALTER TABLE instances RENAME COLUMN lastevent to "lastEvent";

-- Fix amiusage table cases
ALTER TABLE amiusage RENAME COLUMN lastused to "lastUsed";
