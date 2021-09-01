# bash
* Check dir contents, size: du -h dirpath/
* Redirection
    * A single right arrow redirects standard out to file, e.g. ls > output_of_ls.txt
    * Ampersand includes standard error: ls &> stdout_and_stderr.txt
    * Double arrows are useful for concatenation: ls >> add_stdout_to_end_of_this_file.txt
* Shorthand, style
    * tilde ~ expands to users home directory
    * periods . precede hidden file names
    * a period by itself . expands to the current working directory
* Finding things
    * grep to find string in all files in folder: grep -nr 'saveMultiArray' path_to_folder
    * Find file anywhere on system: find / -name "filename"
* Permissions
    * chmod is main permissions command
    * Let owner read/write, group read/write: chmod u=rw,g=rw logs/exact_nat_spark_test1.log
    * Restrict to owner read/write: chmod 600 file_name_here

# TI github (location of 2020 DAS repositories)
* TI github URL: https://github.ti.census.gov/CB-DAS/
* Example clone command: git clone git@github.ti.census.gov:CB-DAS/das-pl94-topdown.git .
* Example ssh setup:
    * eval "$(ssh-agent -s)"
    * ssh-keygen -t rsa -b 4096
    * (manually copy/paste public key into relevant section in GitHub)
    * ssh-add path_to_no_suffix_private_key_file

# AWS browser-based console GUI:
* User names typically look like: u-JBID
* Initial passwords given by Kishore Kakani, Roy D Ashley, Donald Badrak (CSVD), then reset by user regularly. (Integration with network password is pending, but has been pending a long time.)
* Two-factor authentication required (use Authy if no smartphone; Authy or GAuth work if smartphone)
* AWS console log-in URL: https://census-do1.signin.amazonaws-us-gov.com/console

# Using AWS from Linux (EMR, r2)
* Configuration of AWS CLI
    * Use command: aws configure
    * Not needed in EMR, but needed in research2 to read/write s3
    * Region should be: us-gov-west-1
    * Set output to: json
    * Get access, secret key values from Kishore Kakani (CSVD)
    * After config, settings are stored at
        * ~/.aws/config
        * ~/.aws/credentials
* AWS CLI location
    * /apps/Anaconda/bin/aws on r2
    * /usr/bin/aws on EMR nodes
* Download from s3 location
    * /apps/Anaconda/bin/aws s3 cp s3://v-vol-s3-emr-adrm-das-test-1/scripts/test_gurobi_spark.py . --sse aws:kms --sse-kms-key-id KMS_key_here
    * Can get KMS key from Kishore Kakani (CSVD) or Philip Leclerc (CDAR)
    * The KMS key is used for encryption/decryption with the CDAR s3 bucket, v-vol-s3-emr-adrm-das-test-1
    * Reverse command to upload
* To create a "folder" in s3
    * Note that s3 does not technically have folders, just binary objects with names that optionally may include forward slashes
    * AWS GUI console automatically presents binary objects with slashes in names as if they were folders, for user convenience
    * If you really really really want to create a "folder", you can do something like: /apps/Anaconda3/bin/aws s3api put-object --bucket v-vol-s3-emr-adrm-das-test-1 --key output/phils_first_folder_dawww/ --ssekms-key-id kms_key_here --server-side-encryption aws:kms
    * But there's not really any point to creating a "folder" by itself, just upload objects
* boto3 is a Python module that provides a within-Python API alternative to AWS CLI
    * Get boto3: import boto3
    * Get s3 resource: s3 = boto3.resource('s3')
    * Show all s3 buckets visible to this user
        * for bucket in s3.buckets.all():
        * print(bucket.name)
        * boto3 uses files in ~/.aws/ to identify user
    * Get specific bucket: bkt = s3.Bucket('v-vol-s3-emr-adrm-das-test-1')
    * Download file: bkt.download_file('scripts/sparkPi.py','hiimasparkpi')
    * Upload file: bkt.upload_file(Filename='is_this_dog.txt.npy',Key='scripts/yes_this_is_dog.npy',ExtraArgs={'ServerSideEncryption':'aws:kms','SSEKMSKeyId':'kms_key_here'})
    * Numpy npy binary objects are annoying to download properly, but it can be done like:
        * obj = client.get_object(Bucket='v-vol-s3-emr-adrm-das-test-1',Key='scripts/yes_this_is_dog.npy')
        * arr = numpy.load(BytesIO(obj['Body'].read()))
    * Note: s3 upload-download is more difficult from within spark executors (which is to say Phil has yet to succeed at it), regardless of whether AWS CLI or boto3 is used
* To move between r2 and EMR
    * EMR firewall disallows access to TI GitHub, so we use recursive AWS CLI cps with exclude flags to move desired files
        * r2 to s3: /apps/Anaconda3/bin/aws s3 cp code/ s3://v-vol-s3-emr-adrm-das-test-1/your_s3_location_here/ --recursive --exclude '*.lic' --exclude '*~' --exclude '*/logs/*' --exclude '*/confs.json' --exclude '*/math_programs/*' --exclude '*/misc_code/*' --exclude '*/metastore_db/*' --exclude '*/pbs_output/*' --exclude '__pycache__/*' --exclude '*/opt/__pycache__/*' --sse aws:kms --sse-kms-key-id kms_key_here
        * s3 to emr: aws s3 cp s3://v-vol-s3-emr-adrm-das-test-1/your_s3_location_here/ das-pl94-topdown/ --recursive --sse aws:kms --sse-kms-key-id kms_key_here
        * emr to s3: aws s3 cp das-pl94-topdown/ s3://v-vol-s3-emr-adrm-das-test-1/your_s3_location_here/ --recursive --exclude '*~' --exclude '*.swp' --exclude '*/math_programs/*' --exclude '*/misc_code/*' --exclude '*/metastore_db/*' --exclude '*/pbs_output/*' --exclude '*/__pycache__/*' --exclude '*/opt/__pycache__/*' --sse aws:kms --sse-kms-key-id kms_key_here
        * s3 to r2: aws s3 cp s3://v-vol-s3-emr-adrm-das-test-1/your_s3_location_here/ das-pl94-topdown/ --recursive --exclude '*.lic' --exclude '*~' --exclude '*/logs/*' --exclude '*confs.json' --exclude 'math_programs/*' --exclude 'misc_code/*' --exclude 'metastore_db/*' --exclude 'pbs_output/*' --exclude '__pycache__/*' --exclude 'opt/__pycache__/*' --sse aws:kms --sse-kms-key-id kms_key_here

# EMR, ec2, AWS specs
* Detailed, searchable ec2 node characteristics site: https://www.ec2instances.info/
* AWS ec2 on-demand per-node, per-hour pricing (set region to AWS GovCloud (US)): https://aws.amazon.com/ec2/pricing/on-demand/
* AWS emr additional per-node, per-hour pricing (set region to AWS GovCloud (US)): https://aws.amazon.com/emr/pricing/
* AWS s3 read-write and storage pricing: https://aws.amazon.com/s3/pricing/

# Using EMR
* To get into head/master node of cluster to do work
    * ssh into an EMR node: ssh -i pem_file_name.pem hadoop@10.193.ABC.XYZ
    * pem file provided by Kishore Kakani, Roy D Ashley Donald Badrak (CSVD)
    * pem file also obtainable from Philip Leclerc (CDAR), Robert Ashmead (CSVD)
    * For ssh into head node to succeed, must change pem file permissions like: chmod 600 pem_file_name.pem
* To submit a python script to the EMR cluster
    * After ssh-ing into the head node, do: spark-submit test_gurobi_spark.py --master yarn
    * Many other options may be set, see eg code/spark_driver_intermediary.py
* Basic commands for yarn, cluster resource manager
    * See non-master nodes visible to yarn: yarn node -list
    * See applications currently running: yarn application -list
    * See stdout, stderr logs from nodes: yarn logs -applicationId application_1514321948519_0014
    * Typical application id looks like: application_1428487296152_25597 (see run logs or yarn app list to find ids)
    * Kill running application: yarn application -kill applicationid
* rsync can be used to synchronize directories on multiple nodes
    * Example from command line: rsync -avz -e "ssh -i your_pem_file_here.pem" abs_path_to/das-pl94-topdown/ hadoop@node_address:abs_path_to/das-pl94-topdown/
    * See run_spark.py for example that automates this synchronization with python subprocess
    * We do this because some of our code assumes the same code directory structure on all nodes
    * Need for this could probably be avoided with some rewriting of code and use of spark pyfiles argument, which tells spark which python files to make available for import in executors

# spark weburl GUI
* On research2, to access weburl spark GUI
    * Command: firefox http://172.24.106.20:4040 
    * Where IP is one of the research2 IP addresses
        * 172.24.106.16 (2-1/head node)
        * 172.24.106.17 (2-2)
        * 172.24.106.18 (2-3)
        * 172.24.106.19 (2-4)
        * 172.24.106.20 (2-5)
        * 172.24.106.21 (2-6)
        * 172.24.106.22 (2-7)
* Have yet to get weburl spark GUI functioning in EMR

# PBSPro
* General reference (thanks to William Sexton, CDAR): http://docs.adaptivecomputing.com/torque/4-1-4/Content/topics/commands/qstat.htm
* To get detailed info on all running jobs: qstat -f
    * Includes URLs of nodes running jobs
    * Includes snapshot of RAM, CPU usage per job
    * Often lengthy, recommend redirecting to file like: qstaf -f > this_is_a_long_file.txt
* See nodes visible to PBSPro with basic hardware specs: pbsnodes -a

# vim
* Replace on all lines: :%s/stuff/new_stuff/g
* Count occurrences throughout file: :%s/stuff//gn

# EMR node notes
* Current nodes of choice
    * m4.large is probably more than enough for master node
    * m4.16xlarge are very good for core nodes, roughly 4.27 USD per hr with EMR price included
    * Not clear we need any task nodes (they do not participate in use of Hadoop Distributed File System data daemon for long-term storage, and s3 storage is cheap, although read-write requests are less so)
* Prior experiments
    * Used c3.8xlarge compute-optimized nodes heavily, much less effective than m4.16xlarge, both absolutely and per-dollar
