"""
aws.py

A collection of functions useful for working with aws.
Originally written to use the aws cli, slowly being ported to boto3.
(Originally boto3 would not run in the Census environment.)

Also includes support for the Census proxy. We discovered through
trial-and-error that we wanted to proxy https but not http for some but not all services.
"""


import json
import os
import subprocess
import urllib.request
import socket
import boto3
import sys

HTTP_PROXY='HTTP_PROXY'
HTTPS_PROXY='HTTPS_PROXY'
BCC_HTTP_PROXY  = 'BCC_HTTP_PROXY'
BCC_HTTPS_PROXY = 'BCC_HTTPS_PROXY'
NO_PROXY='NO_PROXY'
debug=False

def proxy_on(http=True, https=True):
    if http:
        os.environ[HTTP_PROXY]  = os.environ[BCC_HTTP_PROXY]
    if https:
        os.environ[HTTPS_PROXY] = os.environ[BCC_HTTPS_PROXY]

def proxy_off():
    if HTTP_PROXY in os.environ:
        del os.environ[HTTP_PROXY]
    if HTTPS_PROXY in os.environ:
        del os.environ[HTTPS_PROXY]

class Proxy:
    """Context manager that enables the Census proxy. By default http is not proxied and https is proxied.
    This allows AWS IAM Roles to operate (since they seem to be enabled by http) but we can reach the
    endpoint through the HTTPS proxy (since it's IP address is otherwise blocked).

    This took a long time to figure out.

    Additionally, the IAM role of the server or cluster this code is running on must be able to access the
    proxy. Otherwise, it will give a botocore.exceptions.NoCredentialsError: Unable to locate credentials exception.

    Example -
    .. highlight: python
    import boto3
    from ctools.aws import Proxy

    url = "[INSERT VALID AWS URL HERE]"

    if __name__ == "__main__":

        with Proxy() as p:
            client = boto3.client('sqs')
            response = client.receive_message(
                QueueUrl=url,
                AttributeNames=[
                    'All',
                ],
                MaxNumberOfMessages=10,
                MessageAttributeNames=[
                    'All'
                ],
                VisibilityTimeout=1,
                WaitTimeSeconds=1
            )
    """

    def __init__(self, http=False, https=True):
        self.http = http
        self.https = https

    def __enter__(self):
        proxy_on(http=self.http, https=self.https)
        return self

    def __exit__(self, *args):
        proxy_off()


def get_url(url, context=None, ignore_cert=False, timeout=None):
    if ignore_cert:
        import ssl
        context = ssl._create_unverified_context()

    import urllib.request
    with urllib.request.urlopen(url, context=context, timeout=timeout) as response:
        return response.read().decode('utf-8')

def get_url_json(url, **kwargs):
    return json.loads(get_url(url, **kwargs))

def user_data():
    try:
        return get_url_json("http://169.254.169.254/2016-09-02/user-data/")
    except (TimeoutError, urllib.error.URLError) as e:
        return {}

def instance_identity():
    try:
        return get_url_json('http://169.254.169.254/latest/dynamic/instance-identity/document')
    except (TimeoutError, urllib.error.URLError) as e:
        # Not running on Amazon; return bogus info
        null = None
        return {"accountId": "999999999999",
                "architecture": "x86_64",
                "availabilityZone": "xx-xxxx-0x",
                "billingProducts": null,
                "devpayProductCodes": null,
                "marketplaceProductCodes": null,
                "imageId": "ami-00000000000000000",
                "instanceId": "i-00000000000000000",
                "instanceType": "z9.unknown",
                "kernelId": null,
                "pendingTime": "1980-01-01T00:00:00Z",
                "privateIp": "127.0.0.1",
                "ramdiskId": null,
                "region": "xx-xxxx-0",
                "version": "2017-09-30"}

def ami_id():
    return get_url('http://169.254.169.254/latest/meta-data/ami-id')


def show_credentials():
    """This is mostly for debugging"""
    subprocess.call(['printenv'])
    subprocess.call(['aws', 'configure', 'list'])

def get_ipaddr():
    try:
        return get_url("http://169.254.169.254/latest/meta-data/local-ipv4")
    except (TimeoutError, urllib.error.URLError) as e:
        socket.gethostbyname(socket.gethostname())

def instanceId():
    return instance_identity()['instanceId']


if __name__=="__main__":
    print("instance identity:")
    doc = instance_identity()
    for (k, v) in doc.items():
        print("{}: {}".format(k, v))
    print("AMI ID: {}".format(ami_id()))
