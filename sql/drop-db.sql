-- Remove all the things in our database.  Basically, we just want a 'clean'
-- slate so that our unit tests can run
DROP TRIGGER IF EXISTS update_spotrequests_touched ON spotrequests;
DROP TRIGGER IF EXISTS update_instances_touched ON instances;
DROP TABLE IF EXISTS spotrequests;
DROP TABLE IF EXISTS instances;
DROP TABLE IF EXISTS cloudwatchlog;
DROP TABLE IF EXISTS amiusage;
DROP FUNCTION IF EXISTS update_touched();
