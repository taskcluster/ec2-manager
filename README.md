# EC2 Manager
This is an experiment to try to write a better EC2 manager for the Taskcluster provisioning system.  This system is designed to do the minimal amount of calling
to the describeSpotInstanceRequest and describeInstances EC2 api endpoints, rather using the CloudWatch Events stream to monitor for state changes of running
instances.

## Architecture
This application is comprised of the following major components:

1. '''TODO''' REST Api which is used to run the requestSpotInstance endpoint
1. script which sets up all the CloudWatch Events rules and targets and the backing SQS queues
1. SQS Queue Listener which will listen for the CloudWatch events and put them into a state database
1. A state database which stores mininal information about state
1. A way to bootstrap state from existing state into the state database (maybe)

Worker types association will be managed in this component

### Rest API
This is not implemented yet, but it will provide a couple of basic endpoints.

  * `requestSpotInstance(workerType, launchSpecification)` -- Request a spot instance.   This will ensure that the instance and spot request are associated with the workerType and will take
     the return value of the actual EC2 call to insert into the database as the point of record
  * `cancelSpotRequest(spotInstanceRequestId)` -- Request that a spot instance request is cancelled
  * `killInstance(instanceId)` -- Request that an instance is killed
  * `getState(workerType)` -- Return a view of state for a given worker type

When `requestSpotInstance` is called, the response will be stored in the State Database to store the static values.  We'll have to store the spot request id in a list of ids to continuously poll the describeSpotInstanceRequests api, but because it'll only be until either the request is fulfilled or rejected, we'll be able to poll it way less.

### Setup script
This script will ensure that there's a properly configured rule named `ec2-event-state-transitions` which has a target `ec2-event-queue` which is an SQS Queue in each of the regions.

### SQS Queue Listener
This program will listen for state transitions and call into the state database to update instance state.  This program will also poll the EC2 describeSpotInstanceRequests API to check on yet-to-be-completed spot instance requests.  When an instance is found for the first time, we'll need to 

When a new instance is found which does not exist in the state database, we should look up its information and check if the spot request is fulfilled, and if not mark it as such

### State Database
The database will be stored in Postgres and has tables described in this repository at `sql/create-db.sql`

#### Instances Table
This table will store information about instances.  It will have the following fields:

#### Spot Requests Table
This table will store information about spot requests

### State Bootstrapper
We need to be able to take existing state and put it into the state database.  Both as a way to handle Queue Listener bugs, but also as a maintainance tool and to check the consistency of our state
