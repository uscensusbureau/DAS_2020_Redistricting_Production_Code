import json
import errno
import os
import tempfile
import sys
import subprocess
import time
import re
import string
from collections import defaultdict
import queue, threading
import random
import logging

from urllib.parse import urlparse

import boto3
import botocore
import botocore.exceptions

# note: boto3.resource acquire is not threadsafe, so we create this mutex
boto3_resource_mutex = threading.Lock()
s3_resources = {}               # per thread

S3_RETRIES = 10                 # if there is an error, retry this many times

def s3_resource():
    """Return an s3 resource for this thread."""
    if threading.get_ident() not in s3_resources:
        boto3_resource_mutex.acquire()
        s3_resources[ threading.get_ident() ] = boto3.resource('s3')
        boto3_resource_mutex.release()
    return s3_resources[ threading.get_ident() ]

def s3_object(bucket,key):
    """This sometimes get NoCredentialsError, in which case we retry"""
    retries = 0
    while True:
        try:
            return s3_resource().Object(bucket,key)
        except botocore.exceptions.NoCredentialsError as e:
            print("NoCredentialsError. Waiting and retrying",file=sys.stderr)
            if retries>10:
                raise
            time.sleep( random.uniform(0,0.5))
            retries += 1



#
# This creates an S3 file that supports seeking and caching.
#

_LastModified = 'LastModified'
_ETag = 'ETag'
_StorageClass = 'StorageClass'
_Key = 'Key'
_Size = 'Size'
_Prefix = 'Prefix'

READ_CACHE_SIZE = 4096  # big enough for front and back caches
MAX_READ = 65536 * 16
global debug
debug = False

READTHROUGH_CACHE_DIR = '/mnt/tmp/s3cache'

AWS_CLI_LIST = ['/usr/bin/aws', '/usr/local/bin/aws', '/usr/local/aws/bin/aws']
AWS_CLI  = None

def is_hexadecimal(s):
    """Return true if s is hexadecimal string"""
    if isinstance(s, str)==False:
        return False
    elif len(s)==0:
        return False
    elif len(s)==1:
        return s in string.hexdigits
    else:
        return all([is_hexadecimal(ch) for ch in s])

def awscli():
    global AWS_CLI
    if AWS_CLI is None:
        for path in AWS_CLI_LIST:
            if os.path.exists(path):
                AWS_CLI = path
                return AWS_CLI
        raise RuntimeError("Cannot find aws executable in " +str(AWS_CLI_LIST))
    return AWS_CLI


def get_bucket_key(loc):
    """Given a location, return the (bucket,key)"""
    p = urlparse(loc)
    if p.scheme == 's3':
        return p.netloc, p.path[1:]
    if p.scheme == '':
        if p.path.startswith("/"):
            (ignore, bucket, key) = p.path.split('/', 2)
        else:
            (bucket, key) = p.path.split('/', 1)
        return bucket, key
    assert ValueError("{} is not an s3 location".format(loc))


def aws_s3api(cmd, debug=False):
    aws = awscli()
    fcmd = [aws, 's3api', '--output=json'] + cmd
    if debug:
        sys.stderr.write(" ".join(fcmd))
        sys.stderr.write("\n")

    try:
        p = subprocess.Popen(fcmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out, err) = p.communicate()
        if p.returncode==0:
            if out==b'':
                return out
            try:
                return json.loads(out.decode('utf-8'))
            except json.decoder.JSONDecodeError as e:
                print(e, file=sys.stderr)
                print("out=", out, file=sys.stderr)
                raise e
        else:
            err = err.decode('utf-8')
            if 'does not exist' in err:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(cmd))
            else:
                raise RuntimeError("aws_s3api. cmd={} out={} err={}".format(cmd, out, err))
    except TypeError as e:
        raise RuntimeError("s3 api {} failed data: {}".format(cmd, e))

    if not data:
        return None
    try:
        return json.loads(data)
    except (TypeError, json.decoder.JSONDecodeError) as e:
        raise RuntimeError("s3 api {} failed data: {}".format(cmd, data))

def getsize(bucket, key):
    return s3_resource().Object(bucket,key).content_length

def put_object(bucket, key, fname, use_acl=False):
    """Given a bucket and a key, upload a file"""
    assert os.path.exists(fname)
    if use_acl:
        return aws_s3api(['put-object', '--bucket', bucket, '--key', key, '--body', fname, '--acl', 'bucket-owner-full-control'])

    return aws_s3api(['put-object', '--bucket', bucket, '--key', key, '--body', fname])

def put_s3url(s3url, fname, use_acl=False):
    """Upload a file to a given s3 URL"""
    (bucket, key) = get_bucket_key(s3url)
    return put_object(bucket, key, fname, use_acl)

def get_object(bucket, key, fname):
    """Given a bucket and a key, download a file"""
    if os.path.exists(fname):
        raise FileExistsError(fname)
    return aws_s3api(['get-object', '--bucket', bucket, '--key', key, fname])

def head_object(bucket, key):
    """Wrap the head-object api"""
    return aws_s3api(['head-object', '--bucket', bucket, '--key', key])


def delete_object(bucket, key):
    """Wrap the delete-object api"""
    return aws_s3api(['delete-object', '--bucket', bucket, '--key', key])

def delete_s3url(s3url):
    (bucket, key) = get_bucket_key(s3url)
    return delete_object(bucket, key)


PAGE_SIZE = 1000
MAX_ITEMS = 1000


def list_objects(bucket, prefix=None, limit=None, delimiter=None):
    """Returns a generator that lists objects in a bucket. Returns a list of dictionaries, including Size and ETag"""

    # handle the case where an S3 URL is provided instead of a bucket and prefix
    if bucket.startswith('s3://') and (prefix is None):
        (bucket, prefix) = get_bucket_key(bucket)

    next_token = None
    total = 0
    while True:
        cmd = ['list-objects-v2', '--bucket', bucket, '--prefix', prefix,
               '--page-size', str(PAGE_SIZE), '--max-items', str(MAX_ITEMS)]
        if delimiter:
            cmd += ['--delimiter', delimiter]
        if next_token:
            cmd += ['--starting-token', next_token]

        res = aws_s3api(cmd)
        if not res:
            return
        if 'Contents' in res:
            for data in res['Contents']:
                yield data
                total += 1
                if limit and total >= limit:
                    return

            if 'NextToken' not in res:
                return  # no more!
            next_token = res['NextToken']
        elif 'CommonPrefixes' in res:
            for data in res['CommonPrefixes']:
                yield data
            return
        else:
            return


def search_objects(bucket, prefix=None, *, name, delimiter='/', limit=None, searchFoundPrefixes=True, threads=20,
                   callback = None):
    """Search for occurences of a name. Returns a list of all found keys as dictionaries.
    @param bucket - the bucket to search
    @param prefix - the prefix to start with
    @param name   - the name being searched for
    @param delimiter - the delimiter that separates names
    @param limit  - the maximum number of names keys to return
    @param searchFoundPrefixes - If true, do not search for prefixes below where name is found.
    @param threads - the number of Python threds to use. Note that this is all in the same process.
    """

    if limit is None:
        limit = sys.maxsize  # should be big enough

    # accumulates all the objects to be returned by the main thread
    # It's added to by the worker thread.
    found = []
    s3client = boto3.client('s3')
    def worker():
        while True:
            prefix = q.get()
            if prefix is None:
                break
            found_prefixes = []
            found_names = 0
            paginator = s3client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
            for page in pages:
                for obj in page.get('Contents',[]):
                    if len(found) > limit:
                        break
                    if (name is None) or (os.path.basename(obj['Key'])==name):
                        if callback:
                            callback(obj)
                        else:
                            found.append(obj)
                for obj in page.get('CommonPrefixes',[]):
                    if len(found) > limit:
                        break
                    q.put(obj['Prefix'])
            q.task_done()

    q = queue.Queue()
    thread_pool = []
    for i in range(threads):
        t = threading.Thread(target=worker)
        t.start()
        thread_pool.append(t)
    q.put(prefix)

    # block until all tasks are done
    q.join()

    # stop workers
    for i in range(threads):
        q.put(None)
    for t in thread_pool:
        t.join()
    return found


def etag(obj):
    """Return the ETag of an object. It is a known bug that the S3 API returns ETags wrapped in quotes
    see https://github.com/aws/aws-sdk-net/issue/815"""
    etag = obj['ETag']
    if etag[0] == '"':
        return etag[1:-1]
    return etag



def object_sizes(sobjs):
    """Return an array of the object sizes"""
    return [obj['Size'] for obj in sobjs]


def sum_object_sizes(sobjs):
    """Return the sum of all the object sizes"""
    return sum(object_sizes(sobjs))


def any_object_too_small(sobjs):
    """Return if any of the objects in sobjs is too small"""
    MIN_MULTIPART_COMBINE_OBJECT_SIZE = 0
    return any([size < MIN_MULTIPART_COMBINE_OBJECT_SIZE for size in object_sizes(sobjs)])


def download_object(tempdir, bucket, obj):
    """Given a dictionary that defines an object, download it, and set the fname property to be where it was downloaded"""
    if 'fname' not in obj:
        obj['fname'] = tempdir + "/" + os.path.basename(obj['Key'])
        get_object(bucket, obj['Key'], obj['fname'])


def concat_downloaded_objects(obj1, obj2):
    """Concatenate two downloaded files, delete the second"""
    # Make sure both objects exist
    assert os.path.exists(obj1['fname'])
    assert os.path.exists(obj2['fname'])

    # Concatenate with cat  (it's faster than doing it in Python)
    subprocess.run(['cat', obj2['fname']], stdout=open(obj1['fname'], 'ab'))

    # Update obj1
    obj1['Size'] += obj2['Size']
    if 'ETag' in obj1:  # if it had an eTag
        del obj1['ETag']  # it is no longer valid
    os.unlink(obj2['fname'])  # remove the second file
    return


class S3File:
    """Open an S3 file that can be seeked. This is done by caching to the local file system."""

    def __init__(self, name, mode='rb'):
        self.name = name
        self.url = urlparse(name)
        if self.url.scheme != 's3':
            raise RuntimeError("url scheme is {}; expecting s3".format(self.url.scheme))
        self.bucket = self.url.netloc
        self.key = self.url.path[1:]
        self.fpos = 0
        self.tf = tempfile.NamedTemporaryFile()
        self.s3client = boto3.client('s3')
        self.obj    = self.s3client.get_object(Bucket = self.bucket, Key=self.key)
        self.length = self.obj['ContentLength']
        self.ETag   = self.obj['ETag']

        # Load the caches
        self.frontcache = self._readrange(0, READ_CACHE_SIZE)  # read the first 1024 bytes and get length of the file
        if self.length > READ_CACHE_SIZE:
            self.backcache_start = self.length - READ_CACHE_SIZE
            if debug:
                print("backcache starts at {}".format(self.backcache_start))
            self.backcache = self._readrange(self.backcache_start, READ_CACHE_SIZE)
        else:
            self.backcache = None

    def _readrange(self, start, length):
        # This is gross; we copy everything to the named temporary file, rather than a pipe
        # because the pipes weren't showing up in /dev/fd/?
        # We probably want to cache also... That's coming
        resp = self.s3client.get_object(Bucket = self.bucket, Key=self.key, Range=f'bytes={start}-{start+length-1}')
        data = resp['Body'].read()
        return data

    def __repr__(self):
        return "FakeFile<name:{} url:{}>".format(self.name, self.url)

    def read(self, length=-1):
        # If length==-1, figure out the max we can read to the end of the file
        if length == -1:
            length = min(MAX_READ, self.length - self.fpos + 1)

        if debug:
            print("read: fpos={}  length={}".format(self.fpos, length))
        # Can we satisfy from the front cache?
        if self.fpos < READ_CACHE_SIZE and self.fpos + length < READ_CACHE_SIZE:
            if debug:
                print("front cache")
            buf = self.frontcache[self.fpos:self.fpos + length]
            self.fpos += len(buf)
            if debug:
                print("return 1: buf=", buf)
            return buf

        # Can we satisfy from the back cache?
        if self.backcache and (self.length - READ_CACHE_SIZE < self.fpos):
            if debug:
                print("back cache")
            buf = self.backcache[self.fpos - self.backcache_start:self.fpos - self.backcache_start + length]
            self.fpos += len(buf)
            if debug:
                print("return 2: buf=", buf)
            return buf

        buf = self._readrange(self.fpos, length)
        self.fpos += len(buf)
        if debug:
            print("return 3: buf=", buf)
        return buf

    def seek(self, offset, whence=0):
        if debug:
            print("seek({},{})".format(offset, whence))
        if whence == 0:
            self.fpos = offset
        elif whence == 1:
            self.fpos += offset
        elif whence == 2:
            self.fpos = self.length + offset
        else:
            raise RuntimeError("whence={}".format(whence))
        if debug:
            print("   ={}  (self.length={})".format(self.fpos, self.length))

    def seekable(self):
        return True

    def tell(self):
        return self.fpos

    def write(self):
        raise RuntimeError("Write not supported")

    def flush(self):
        raise RuntimeError("Flush not supported")

    def close(self):
        return

#
# S3 Cache
#

# Tools for reading and write files from Amazon S3 without boto or boto3
# http://boto.cloudhackers.com/en/latest/s3_tut.html
# but it is easier to use the AWS cli, since it's configured to work.
#
# This could be redesigned to simply use the S3File() above
# Todo: redesign so that it can be used in a "with" statement

class s3open:
    def __init__(self, path, mode="r", encoding=sys.getdefaultencoding(), fsync=False):
        """
        Open an s3 file for reading or writing. Can handle any size, but cannot seek.
        We could use boto3 or one of these packages:
               * https://s3fs.readthedocs.io/en/latest/
               * http://boto.cloudhackers.com/en/latest/s3_tut.html

        This is legacy code from when we had systems that would not work with boto3.

        2020-02-17 - Removed file cache
        :param fsync: if True and mode is writing, use object-exists to wait for the object to be created on exit.
        """
        if not path.startswith("s3://"):
            raise ValueError("Invalid path: " + path)

        if "b" in mode:
            encoding = None

        self.path = path
        self.mode = mode
        self.encoding = encoding
        self.fsync = fsync

        assert 'a' not in mode
        assert '+' not in mode

        if "r" in mode:
            self.p = subprocess.Popen([awscli(), 's3', 'cp', '--quiet', path, '-'],
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE,
                                      encoding=encoding)
            self.file_obj = self.p.stdout

        elif "w" in mode:
            self.p = subprocess.Popen([awscli(), 's3', 'cp', '--quiet', '-', path],
                                      stdin=subprocess.PIPE, encoding=encoding)
            self.file_obj = self.p.stdin
        else:
            raise RuntimeError("invalid mode:{}".format(mode))

    def __enter__(self):
        return self.file_obj

    def __exit__(self, exception_type, exception_value, traceback):
        self.file_obj.close()
        if self.fsync and "w" in self.mode:
            if self.p.wait() != 0:
                raise RuntimeError(self.p.stderr.read())
            self.waitObjectExists()

    def waitObjectExists(self):
        # s3api has wait object-exists, but it doesn't implement waiters. They appear to be implemented in the cli itself.
        if self.fsync and "w" in self.mode:
            (bucket, key) = get_bucket_key(self.path)
            aws_s3api(['wait', 'object-exists', '--bucket', bucket, '--key', key])

    # The following 4 methods are only needed for direct use of s3open as object, outside with-statement, rather than as a context manager
    def __iter__(self):
        return self.file_obj.__iter__()

    def read(self, *args, **kwargs):
        return self.file_obj.read(*args, **kwargs)

    def write(self, *args, **kwargs):
        return self.file_obj.write(*args, **kwargs)

    def readline(self, *args, **kwargs):
        return self.file_obj.readline(*args, **kwargs)

    def close(self):
        self.waitObjectExists()
        return self.file_obj.close()


def s3exists(path):
    """
    Return True if the S3 file exists.
    https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
    """
    (bucket,key) = get_bucket_key(path)
    error = None
    for retry in range(S3_RETRIES):
        try:
            s3_resource().Object(bucket, key).load()
            return True
        except botocore.exceptions.NoCredentialsError as e:
            logging.warning("%s retry=%d",e,retry)
            error = e
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code']=='404':
                return False
            else:
                # Something else has gone wrong
                raise
    raise RuntimeError(f"Retries Exceeded: {error}")


def s3rm(path):
    """Remove an S3 object"""
    (bucket, key) = get_bucket_key(path)
    res = aws_s3api(['delete-object', '--bucket', bucket, '--key', key])
    print("res:", res)
    # delete-object return no output in Staging, even though it successfully deleted the key
    # This is likely because bucket versioning is not enabled in Staging
    # See https://wiki.outscale.net/display/EN/Removing+Objects+from+a+Bucket
    # This results in an empty byte string when returned from aws_s3api
    if res == b'' or res['DeleteMarker'] == True:
        return

    raise RuntimeError("Unknown response from delete-object: {}".format(res))

class DuCounter:
    def __init__(self):
        self.total_bytes = 0
        self.total_files = 0

    def count(self, bytes_):
        self.total_bytes += bytes_
        self.total_files += 1

def print_du(root):
    """Print a DU output using aws cli to generate the usage"""
    prefixes = defaultdict(DuCounter)

    cmd = [awscli(), 's3', 'ls', '--recursive', root]
    print(" ".join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, encoding='utf-8')
    part_re = re.compile(r"(\d\d\d\d-\d\d-\d\d) (\d\d:\d\d:\d\d)\s+(\d+) (.*)")
    total_bytes = 0
    MiB = 1024 *1024
    try:
        for (ct, line) in enumerate(p.stdout):
            parts       = part_re.search(line)
            if parts is None:
                print("Cannot parse: ", line, file=sys.stderr, flush=True)
                continue
            bytes_      = int(parts.group(3))
            path        = parts.group(4)
            total_bytes += bytes_
            prefixes[os.path.dirname(path)].count(bytes_)

            if ct %1000==0:
                print(f"files: {ct}  MiB: {int(total_bytes/MiB):,}  {parts.group(4)}", flush=True)
    except KeyboardInterrupt as e:
        print("*** interrupted ***")

    print(f"Total lines: {ct}  MiB: {int(total_bytes/MiB):,},")
    fmt1 = "{:>10}{:>20}  {}"
    fmt2 = "{:>10}{:>20,}  {}"
    print()
    print("Usage by prefix:")
    print(fmt1.format('files', 'bytes', 'path'))
    for path in sorted(prefixes):
        print(fmt2.format(prefixes[path].total_files,
                          prefixes[path].total_bytes,
                          path))
    print("")
    print("Top 20 by size:")
    print(fmt1.format('files', 'bytes', 'path'))
    for (ct, path) in enumerate(sorted(prefixes, key=lambda path: prefixes[path].total_bytes, reverse=True), 1):
        print(fmt2.format(prefixes[path].total_files,
                          prefixes[path].total_bytes,
                          path))
        if ct==20:
            break


if __name__ == "__main__":
    t0 = time.time()
    count = 0
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter,
                            description="Combine multiple files on Amazon S3 to the same file.")
    parser.add_argument("--ls", action='store_true', help="list a s3 prefix")
    parser.add_argument("--du", action='store_true', help='List usage under an s3 prefix')
    parser.add_argument("--delimiter", help="specify a delimiter for ls")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--search", help="Search for something")
    parser.add_argument("--threads", help="For searching, the number of threads to use", type=int, default=20)
    parser.add_argument("roots", nargs="+")
    args = parser.parse_args()
    if args.debug:
        debug = args.debug
    for root in args.roots:
        (bucket, prefix) = get_bucket_key(root)
        if args.ls:
            for data in list_objects(bucket, prefix, delimiter=args.delimiter):
                print("{:18,} {}".format(data[_Size], data[_Key]))
                count += 1
        if args.search:
            what = args.search
            if what=='all':
                what=None
            for data in search_objects(bucket, prefix, name=what, searchFoundPrefixes=False, threads=args.threads):
                print("{:18,} {}".format(data[_Size], data[_Key]))
                count += 1
        if args.du:
            print_du(root)
    t1 = time.time()
    if args.ls or args.search:
        print("Total files:  {}".format(count), file=sys.stderr)
        print("Elapsed time: {}".format(t1 - t0), file=sys.stderr)
