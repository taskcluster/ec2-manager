ALTER TABLE instances ALTER COLUMN id TYPE text;
ALTER TABLE instances ALTER COLUMN "workerType" TYPE text;
ALTER TABLE instances ALTER COLUMN region TYPE text;
ALTER TABLE instances ALTER COLUMN az TYPE text;
ALTER TABLE instances ALTER COLUMN "instanceType" TYPE text;
ALTER TABLE instances ALTER COLUMN state TYPE text;
ALTER TABLE instances ALTER COLUMN "imageId" TYPE text;

ALTER TABLE terminations ALTER COLUMN id TYPE text;
ALTER TABLE terminations ALTER COLUMN "workerType" TYPE text;
ALTER TABLE terminations ALTER COLUMN region TYPE text;
ALTER TABLE terminations ALTER COLUMN az TYPE text;
ALTER TABLE terminations ALTER COLUMN "instanceType" TYPE text;
ALTER TABLE terminations ALTER COLUMN "imageId" TYPE text;
ALTER TABLE terminations ALTER COLUMN code TYPE text;
ALTER TABLE terminations ALTER COLUMN reason TYPE text;


ALTER TABLE awsrequests ALTER COLUMN region TYPE text;
ALTER TABLE awsrequests ALTER COLUMN "requestId" TYPE text;
ALTER TABLE awsrequests ALTER COLUMN method TYPE text;
ALTER TABLE awsrequests ALTER COLUMN service TYPE text;
ALTER TABLE awsrequests ALTER COLUMN code TYPE text;
ALTER TABLE awsrequests ALTER COLUMN message TYPE text;
ALTER TABLE awsrequests ALTER COLUMN "workerType" TYPE text;
ALTER TABLE awsrequests ALTER COLUMN az TYPE text;
ALTER TABLE awsrequests ALTER COLUMN "instanceType" TYPE text;
ALTER TABLE awsrequests ALTER COLUMN "imageId" TYPE text;

ALTER TABLE cloudwatchlog ALTER COLUMN region TYPE text;
ALTER TABLE cloudwatchlog ALTER COLUMN id TYPE text;
ALTER TABLE cloudwatchlog ALTER COLUMN state TYPE text;

ALTER TABLE amiusage ALTER COLUMN region TYPE text;
ALTER TABLE amiusage ALTER COLUMN id TYPE text;






