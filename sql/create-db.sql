-- Set up a PostgreSQL Database for use as the backing
-- data store for the EC2 Manager component

-- Here's the SQL to drop your things
--   DROP TABLE IF EXISTS instances;
--   DROP FUNCTION IF EXISTS update_touched();

-- This function updates the 'touched' column on the table
-- it is tied to to ensure that any time we update the entry
-- we automatically update the touched column
--
-- Based on http://stackoverflow.com/a/26284695
CREATE OR REPLACE FUNCTION update_touched()
RETURNS TRIGGER AS $$
BEGIN
  IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
    NEW.touched = now();
    RETURN NEW;
  ELSE
    RETURN OLD;
  END IF;
END;
$$ language 'plpgsql';

-- instances table contains minimal information on
-- any instances owned by this ec2 manager
CREATE TABLE IF NOT EXISTS instances (
  id TEXT NOT NULL, -- opaque ID per Amazon
  "workerType" TEXT NOT NULL, -- taskcluster worker type
  region TEXT NOT NULL, -- ec2 region
  az TEXT NOT NULL, -- availability zone
  "instanceType" TEXT NOT NULL, -- ec2 instance type
  state TEXT NOT NULL, -- e.g. running, pending, terminated
  "imageId" TEXT NOT NULL, -- AMI/ImageId value
  launched TIMESTAMPTZ NOT NULL, -- Time instance launched
  "lastEvent" TIMESTAMPTZ NOT NULL, -- Time that the last event happened in the api. Used
                                    -- to ensure that we have correct ordering of cloud watch
                                    -- events
  touched TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(id, region)
);
-- Automatically keep instances touched parameter up to date
CREATE TRIGGER update_instances_touched
BEFORE UPDATE ON instances
FOR EACH ROW EXECUTE PROCEDURE update_touched();

-- termination reasons
CREATE TABLE IF NOT EXISTS terminations (
  id TEXT NOT NULL, -- opaque ID per Amazon
  "workerType" TEXT NOT NULL, -- taskcluster worker type
  region TEXT NOT NULL, -- ec2 region
  az TEXT NOT NULL, -- availability zone
  "instanceType" TEXT NOT NULL, -- ec2 instance type
  "imageId" TEXT NOT NULL, -- AMI/ImageId value
  code TEXT, -- the State Reason's code
  reason TEXT, -- the State Reason's string message
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

-- We want to track the calls to runinstances and
-- store the error code if one exists
CREATE TABLE IF NOT EXISTS awsrequests (
  -- Mandatory fields
  region TEXT NOT NULL, -- aws region
  "requestId" TEXT NOT NULL, -- aws request id
  duration INTERVAL NOT NULL, -- time in ms that the request took
  method TEXT NOT NULL, -- the api method run, e.g. runInstances
  service TEXT NOT NULL, -- the service the method was run against, e.g. ec2
  error BOOLEAN NOT NULL, -- true if the request resulted in an error
  called TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- when the API call was initiated

  -- EC2 error data
  code TEXT, -- EC2 api error code
  message TEXT, -- EC2 Api error message

  -- The following are values which can optionally be added where
  -- appropriate
  "workerType" TEXT, -- taskcluster worker type
  az TEXT, -- availability zone
  "instanceType" TEXT, -- ec2 instance type
  "imageId" TEXT, -- AMI/ImageId value

  PRIMARY KEY(region, "requestId")
);

CREATE INDEX ON awsrequests (
  region,
  az,
  "instanceType",
  "workerType",
  code,
  called
) WHERE error=true AND method = 'runInstances';

-- Cloudwatch Events Log
-- We want to keep a log of when every cloud watch event was generated
CREATE TABLE IF NOT EXISTS cloudwatchlog (
  region TEXT, -- ec2 region
  id TEXT, -- opaque ID per amazon
  state TEXT, -- state from message
  generated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  received TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, region, state, generated)
);

-- Amazon Machine Image (ami) usage
CREATE TABLE IF NOT EXISTS amiusage (
  region TEXT NOT NULL, -- ec2 region
  id TEXT NOT NULL, -- opaque ID per Amazon
  "lastUsed" TIMESTAMPTZ NOT NULL, -- most recent usage
  PRIMARY KEY(id, region)
);
