# Runtime Configuration

This document describes the runtime configuration required for the disclosure avoidance systems. The purpose of this document is to allow software developed for use within the Census GovCloud implementation to be run outside on servers such as Amazon's computer Amazon Web Services offering, and on stand-alone systems such as laptops and desktop servers.

## Software Environment

DAS requires the following software to be installed on the operational system:

* Anaconda 3.6 Python distribution (64-bit for Linux)
* Gurobi optimizer

For the development environment, we additionally install:

* git, emacs, tmux

## Environment Variables

The following environment variables are configured on every node of the DAS system.
In the EMR environment, they are configured by the script `/etc/profile.d/census.sh` which is created by an EMR bootstrap script.

| Variable          | Example | Purpose |
|-------------------|---------|---------|
| BOOTSTRAP_VERSION | 2018-07-09 |  ISO-8601 date of the version of the Bootstrap script. |
| DAS_S3ROOT        | s3://uscb-das-files | Specifies the prefix of where S3 files will be stored. |
| DAS_SNSTOPIC      | arn:aws-us-gov:sns:us-gov-west-1:123456:DASTopic | Specifies a SNS topic that will be used to broadcast information about DAS progress. If it is not set, no SNS notification messages will be sent. |
| HTTP_PROXY        | http://proxy.server.com:3000 | Specifies a HTTP proxy server to use for certain AWS commands |
| HTTPS_PROXY        | https://proxy.server.com:3000 | Specifies a HTTPS proxy server to use for certain AWS commands |
| TZ                | EST5EDT | Specifies your timezone |
| GUROBI_HOME       | /apps/gurobi752 | Specifies the home directory of the Gurobi installation | 
| GRB_LICENSE_FILE  | /apps/gurobi752/gurobi_client.lic | Specifies the location of the Gurobi license file |
| GRB_ISV_NAME      | Census | Specifies the ISV name for the Gurobi license |
| GRB_APP_NAME      | DAS    | Specifies the application name for the Gurobi license |
| FRIENDLY_NAME     | smiles | Specifies a friendly name for your cluster |
| CLUSTERID         | j-VAFEKMCK12 | Specifies the AWS EMR Cluster ID. If not set, you may not be running in EMR |
| ISMASTER          | true | true if you are running on the EMR master node |
| ISSLAVE          | false | true if you are running on the EMR non-master node |
| MASTER_IP        | 10.1.2.3 | IP address of the master node |



