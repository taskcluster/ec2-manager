# EC2 Manager
[![Build Status](https://travis-ci.org/taskcluster/ec2-manager.svg?branch=master)](https://travis-ci.org/taskcluster/ec2-manager)
Track, manage and alter the state of an EC2 account with a focus on spot
requests.  The application understands EC2 regions, instances and pools of
instances.  It does not understand Taskcluster Provisioning specifics like
Capacity or Utility factors.

## Architecture EC2-Manager is comprised of X major subsystems:

1. State Database
1. CloudWatch Event listener
1. Spot Instance Request Poller
1. Periodic Housekeeping
1. API

### State Database
The state database is implemented in Postgres and has an interface provided by
the `./lib/state.js` file.  This database is written to be as simple as
possible while providing the data we need with transactional consistency where
appropriate.  Only the absolute minimal amount of information is stored in the
database.

### CloudWatch Event Listener
CloudWatch Events are the primary datasource for information on instance state.
Information about instances is stored in the `instances` table.  Whenever the
state (e.g. `pending`, `running`) changes for an instance, a message is sent
with the instance's id and the new state.  When an event is received for the
creation of an instance, we need to look up some metadata using the
`describeInstances` EC2 API, but we unconditionally delete instance shutdowns.
Whenever we get a message about an instance creation, we ensure that the spot
request it was associated is removed from the list of spot requests we need to
poll.

The messages from CloudWatch Events reach us through an SQS queue.  In the case
that the instance's metadata isn't available through the `decribeInstances`
endpoint, we redeliver the message a number of times.  If it is unsuccesful on
the last attempt, we report it to Sentry and move on.  The periodic
housekeeping will ensure that it is inserted into the state database when
appropriate.

### Spot Instance Request Poller
Whenever a spot instance is requested from the API, we insert relevant metadata
from it into the `spotrequests` table.  Periodically the ec2-manager will check
if any of the outstanding spot requests have been resolved.  This is often not
  done because the request is fulfilled, thus generating a cloudwatch event,
  long before we poll for it.  If this poller discovers a spot request which
  has entered a state where it will not be fulfilled, it is cancelled.

### Periodic Housekeeping
Every hour, the EC2 Manager will request the EC2 API's view of the state.  When
doing this it will kill any instance which has exceeded the absolute maximum
run time.  For all other instances and spot requests, the state returned by the
EC2 API will be used to confirm the view of state that the EC2 Manager has.
Any instance in local state which is not in EC2 API state will be deleted from
local state and any instance in EC2 API state but not local will be added to
local state.

### API
The API provided by EC2 Manager can be used manage EC2 instances.  Of paricular
note is that the endpoint for submitting spot requests requires a fully formed
and valid `LaunchSpecification`.

## Hacking

```
git clone https://github.com/taskcluster/ec2-manager
cd ec2-manager
yarn install --frozen-lockfile --ignore-optional
yarn test
```

Tests require a working PostgreSQL server initialized with the
schema of this project.

To use Docker to start a PostgreSQL server:

```
docker run --rm -p 5432:5432 postgres:9.6
```

This will pull down and run a PostgreSQL server in the local terminal.
It will map port 5432 in the container (the PostgreSQL server) to port
5432 on the Docker host. The container will run in the foreground and take
over the terminal. You can also use `docker run -d` to detach from the
current terminal.

Once the PostgreSQL server is running, create database schemas
necessary to run the tests:

```
psql -h localhost -p 5432 -U postgres -f sql/create-db.sql
```

(The exact hostname - -h argument - may vary from machine to machine.)

Tests will need the `DATABASE_URL` environment variable pointing to
this PostgreSQL server. Set it to something like:

```
export DATABASE_URL=postgres://postgres@localhost:5432/postgres
```

Once `DATABASE_URL` is set to a valid PostgreSQL server, you can run
the tests:

```
yarn test
```

## Deployment notes
When deploying, keep in mind the following:

1. SSH Pubkey used in the LaunchSpecification must match the one configured in
   the EC2 Manager.  If you submit a LaunchSpecification with a different
   public key, it will be rejected.
2. This service is set up to auto deploy to the `ec2-manager-staging` Heroku
   app on pushes to the master branch.  Deploying to production requires
   promotion
