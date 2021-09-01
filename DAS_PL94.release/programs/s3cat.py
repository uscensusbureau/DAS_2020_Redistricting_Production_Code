#!/usr/bin/env python3

"""s3_cat.py:

This program uses the AWS S3 API to concatenate all of the files
within a prefix into a single file.  The components are kept.

The original approach was to use the aws s3 API and combine objects on the back-end. Unfortunately, it turns out that there is a minimum part size of 5MB for S3 objects to be combined. In our testing, just using 'aws s3 cp --recursive' to download the parts, combine them locally, and then upload them together was superior. So that is one of the modes of operation, and it appears to be more resillient than the API-based approach.

References:
https://docs.aws.amazon.com/cli/latest/userguide/using-s3-commands.html
https://aws.amazon.com/blogs/developer/efficient-amazon-s3-object-concatenation-using-the-aws-sdk-for-ruby/
https://docs.aws.amazon.com/cli/latest/reference/s3api/create-multipart-upload.html
https://docs.aws.amazon.com/cli/latest/reference/s3api/upload-part-copy.html
https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
https://docs.aws.amazon.com/cli/latest/reference/s3api/head-object.html
Note- minimum part size is 5MB

"""

import subprocess
import json
import urllib
import urllib.parse
import tempfile
import os
import os.path
import sys
import time
import logging

from os.path import dirname,basename,abspath

try:
    import ctools.s3 as s3
except ImportError as e:
    sys.path.append( os.path.join(dirname(dirname(abspath(__file__))), "das_framework") )
    import ctools.s3 as s3


def get_dvs():
    """Return a dvs object module, or None if we can't find it."""
    try:
        import programs.python_dvs.dvs as dvs
        return dvs
    except ImportError as e:
        pass

    try:
        import python_dvs.dvs as dvs
        return dvs
    except ImportError as e:
        pass

    sys.path.append(os.path.dirname(__file__))
    try:
        import python_dvs.dvs as dvs
        return dvs
    except ImportError as e:
        pass

    return None


TMP_DIR = "/usr/tmp"
MIN_MULTIPART_COMBINE_OBJECT_SIZE = 1024 * 1024 * 5  # amazon says it is 5MB


def aws_s3api(cmd):
    fcmd = ['aws', 's3api', '--output=json'] + cmd
    # print(" ".join(fcmd),file=sys.stderr)
    data = subprocess.check_output(fcmd, encoding='utf-8')
    try:
        return json.loads(data)
    except (TypeError, json.decoder.JSONDecodeError) as e:
        raise RuntimeError("s3 api {} failed data: {}".format(cmd, data))


def get_bucket_key(loc):
    """Given a location, return the bucket and the key"""
    p = urllib.parse.urlparse(loc)
    if p.scheme == 's3':
        return (p.netloc, p.path[1:])
    if p.scheme == '':
        if p.path.startswith("/"):
            (ignore, bucket, key) = p.path.split('/', 2)
        else:
            (bucket, key) = p.path.split('/', 1)
        return (bucket, key)
    assert ValueError("{} is not an s3 location".format(loc))


def put_object(bucket, key, fname):
    """Given a bucket and a key, upload a file"""
    print(f"put_object({bucket},{key},{fname})")
    return aws_s3api(['put-object', '--bucket', bucket, '--key', key, '--body', fname])


def get_object(bucket, key, fname):
    """Given a bucket and a key, upload a file"""
    return aws_s3api(['get-object', '--bucket', bucket, '--key', key, fname])


def head_object(bucket, key):
    """Wrap the head-object api"""
    return aws_s3api(['head-object', '--bucket', bucket, '--key', key])


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


def download_object(tempdir, bucket, obj, verbose=False):
    """Given a dictionary that defines an object, download it, and set the fname property to be where it was downloaded"""
    if verbose:
        print(f"s3cat: download_object({obj})")
    if 'fname' not in obj:
        obj['fname'] = tempdir + "/" + os.path.basename(obj['Key'])
        get_object(bucket, obj['Key'], obj['fname'])


def concat_downloaded_objects(obj1, obj2):
    """Concatenate two downloaded files, delete the second"""
    # Make sure both objects exist
    for obj in [obj1, obj2]:
        if not os.path.exists(obj['fname']):
            raise FileNotFoundError(obj['fname'])

    # Concatenate with cat  (it's faster than doing it in python)
    subprocess.run(['cat', obj2['fname']], stdout=open(obj1['fname'], 'ab'))

    # Update obj1
    obj1['Size'] += obj2['Size']
    if 'ETag' in obj1:  # if it had an eTag
        del obj1['ETag']  # it is no longer valid
    os.unlink(obj2['fname'])  # remove the second file
    return


def get_tmp():
    if ("TMP" not in os.environ) and ("TEMP" not in os.environ) and ("TMP_DIR" not in os.environ):
        return TMP_DIR
    return None


def s3cat_multipart(*, prefix, demand_success, suffix, verbose):
    """This performs s3cat using Amazon's back-end protocols which combines
    objects in S3. The problem is that the objects need to be larger than 5MB each.
    So we literally need to download and combine objects that are smaller than 5MB, then upload
    them back to S3. This was super-paintful to implement correctly, and then we found it was slower
    than just downloading all the data and combining it and uploading it. SO that's what we do.
    This is left here for historical interest."""

    raise RuntimeError("Don't use this without more testing.")
    with tempfile.TemporaryDirectory(dir=get_tmp()) as tempdir:
        if verbose:
            print(f"s3cat({prefix}, demand_success={demand_success}, suffix={suffix}) MULTIPART")

        (bucket, key) = get_bucket_key(prefix)
        destkey = key + suffix  # where the data will go; allows us to add an extension

        # Download all of the objects that are too small.
        # When we download a run, concatenate them.
        # This could be more efficient if we concatentated on download.
        run = None
        nobjs = []  # the new list, after the small ones are deleted

        if verbose:
            print(f"s3cat: Downloading objects smaller than {MIN_MULTIPART_COMBINE_OBJECT_SIZE}B and adjacent objects")

        found_success = False
        total_bytes = 0
        for obj in s3.list_objects(bucket, key):
            total_bytes += obj['Size']
            if obj['Size'] == 0:
                if obj['Key'].endswith("/_SUCCESS"):
                    found_success = True
                    if verbose:
                        print("s3cat: found _SUCCESS")
                continue  # ignore zero length

            if obj['Size'] < MIN_MULTIPART_COMBINE_OBJECT_SIZE:
                download_object(tempdir, bucket, obj, verbose=verbose)
                if run:
                    concat_downloaded_objects(run, obj)
                    continue  # dont process this object anymore
                else:
                    run = obj  # start of a new run
            else:
                run = None  # no longer a run
            nobjs.append(obj)

        if verbose:
            print(f"s3cat: total objects: {len(nobjs)}  total_bytes: {total_bytes}")

        if demand_success and not found_success:
            raise FileNotFoundError(f"s3cat: {prefix}/_SUCCESS not found", file=sys.stderr)

        # Now all of the runs have been collapsed. If any of the objects
        # are still too small, we will need to download the previous "big enough" object and combine them.
        # If there is no previous "big enough" object, then we download the next object and combine them
        prev_big_enough = None
        prepend_object = None
        parts = []

        for obj in nobjs:
            if obj['Size'] < MIN_MULTIPART_COMBINE_OBJECT_SIZE:
                assert obj['fname'] > ''  # make sure that this was downloaded
                # If there is a previous object, download it and combine the current object with the previous
                if prev_big_enough:
                    download_object(tempdir, bucket, prev_big_enough, verbose=verbose)  # make sure it is downloaded
                    download_object(tempdir, bucket, obj, verbose=verbose)
                    concat_downloaded_objects(prev_big_enough, obj, verbose=verbose)
                    continue  # don't process this object anymore; prev is already in nobjs
                # There was no previous object. Remember this object as the prepend object
                assert prepend_object == None  # there should be no prepend object at the moment
                prepend_object = obj
                continue
            if prepend_object:
                # Even though obj is big enough, we need to download it and append it to the prepend_object
                download_object(tempdir, bucket, obj, verbose=verbose)
                concat_downloaded_objects(prepend_object, obj)
                obj = prepend_object  # now we are working wi
                prepend_object = None
            prev_big_enough = obj  # the current object is now big enough to be prepended to
            parts.append(obj)

        # If all of the objects were too small together, then there is nothing in parts and they are
        # all in prepend_object. Process it.
        if prepend_object:
            assert len(parts) == 0
            assert prepend_object['Size'] == total_bytes
            # Just upload the single object, and we're done.
            put_object(bucket, destkey, prepend_object['fname'])
            return f"s3://{bucket}/{destkey}"

        # IF we got here, there should not have been a prepend_object
        assert total_bytes == sum_object_sizes(parts)  # Make sure we didn't lose anybody
        assert prepend_object == None  # make sure that nothing is waiting to be prepended

        # Now we can multipart upload!
        # Some objects will need to be uploaded, others are already uploaded

        upload = aws_s3api(['create-multipart-upload', '--bucket', bucket, '--key', destkey])
        upload_id = upload['UploadId']
        if verbose:
            print(f"starting multipart upload at {time.asctime()}.")

        # Now use upload-part or upload-part-copy for each part
        mpustruct = {"Parts": []}
        mpargs = ['--bucket', bucket, '--key', destkey, '--upload-id', upload_id]
        for (part_number, obj) in enumerate(parts, 1):
            args = mpargs + ['--part-number', str(part_number)]

            if 'fname' in obj:
                # This is on disk, so we need to use upload-part
                cpr = aws_s3api(['upload-part'] + args + ['--body', obj['fname']])
                mpustruct['Parts'].append({"PartNumber": part_number, "ETag": etag(cpr)})
            else:
                # Not on disk, so just combine the part
                cpr = aws_s3api(['upload-part-copy'] + args + ['--copy-source', bucket + "/" + obj['Key']])
                mpustruct['Parts'].append({"PartNumber": part_number, "ETag": etag(cpr['CopyPartResult'])})

        # Complete the transaction
        aws_s3api(['complete-multipart-upload'] + mpargs + ['--multipart-upload', json.dumps(mpustruct)])
        if verbose:
            print(f"multipart upload completed at {time.asctime()}.")
        return f"s3://{bucket}/{destkey}"


def s3cat_download(prefix, *, demand_success=False, suffix="", verbose=False):
    """This version downloads all of the files from S3 under 'prefix' to the local system, concatenates them, and sends them back
    with suffix added.
    This is done with the aws command line program using the subprocess command.

    :param demand_success: if True, requires that a __SUCCESS file be present
    :param suffix:         The suffix to add to the prefix.
    :param verbose:        Prints debugging information.
    """

    def verbprint(cmd):
        logging.info(' '.join(cmd))
        if verbose:
            if len(cmd) < 10:
                print(' '.join(cmd))
            else:
                print(' '.join(cmd[1:10]), '...')

    with tempfile.TemporaryDirectory(dir=get_tmp()) as tempdir:
        verbprint([f"s3cat({prefix}, demand_success={demand_success}, suffix={suffix}) DOWNLOAD"])

        cmd = ['aws', 's3', 'cp', '--quiet', '--no-progress', '--recursive', prefix, tempdir]
        verbprint(cmd)
        subprocess.check_call(cmd)
        if demand_success:
            success_fname = os.path.join(tempdir, "_SUCCESS")
            if not os.path.exists(success_fname):
                raise FileNotFoundError(success_fname)

        with tempfile.NamedTemporaryFile(dir=get_tmp(), mode='wb') as tf:
            # We might have too many parts to fit on a single command line!
            # So we create a shell script that has one line for each file to be combined.
            with tempfile.NamedTemporaryFile(mode='w') as script:
                script.write("#!/bin/bash\n")
                script.write("".join([f"cat {os.path.join(tempdir, name)} >> {tf.name}\n"
                                      for name in sorted(os.listdir(tempdir))]))
                script.flush()
                subprocess.check_call(['/bin/bash', script.name], stdout=tf)

            # upload the combined file
            cmd = ['aws', 's3', 'cp', '--quiet', '--no-progress', tf.name, prefix + suffix]
            verbprint(cmd)
            subprocess.check_call(cmd)
        return prefix + suffix


def s3cat(prefix, *, demand_success=False, suffix="", verbose=False,
          expandvars=True, multipart=False, message=None, debug=False,
          use_dvs=False):
    """
    :param prefix - the prefix we wish to combine
    :param demand_success - if True, raise an error if {prefix}/_SUCCESS is not present.
    :param suffix - the suffix to add to prefix when combining files.
    :param verbose - print progress
    :param expandvars - if $VAR is in prefix, replace with VAR from environment.
                      - replace $$ with str(os.getpid())
    :param multipart - Use the AWS S3 back-end API to combine the files. We implemented this,
                       but then we found that it was somewhat faster to just use 'aws cp' to
                      download all of the files to a temporary location, then combine, then upload.
    @returns the new S3 location
    """

    if expandvars:
        prefix = prefix.replace("$$", str(os.getpid()))
        prefix = os.path.expandvars(prefix)

    if not prefix.startswith("s3://"):
        raise ValueError(f"prefix does not start with 's3://'. prefix: {prefix}")

    if prefix.endswith("/"):
        raise ValueError(f"prefix must not end with '/'. prefix: {prefix}")

    dvs = None
    if use_dvs:
        dvs = get_dvs()
        if not dvs:
            use_dvs = False
        if dvs.instance is None:
            use_dvs = False
            dvs = None

    if use_dvs:
        commit = {}
        if message:
            commit[dvs.COMMIT_MESSAGE] = message
        dc = dvs.DVS(base=commit, debug=debug)
        dc.add_s3_paths_or_prefixes(dvs.COMMIT_BEFORE, [prefix+'/'])

    if not multipart:
        s3path = s3cat_download(prefix, demand_success=demand_success, suffix=suffix, verbose=verbose)
    else:
        raise RuntimeError("Don't use multipart. s3cat_download runs faster")
        s3path = s3cat_multipart(prefix, demand_success=demand_success, suffix=suffix, verbose=verbose)

    if use_dvs:
        from programs.python_dvs.dvs import DVSException
        dc.add_s3_paths_or_prefixes(dvs.COMMIT_AFTER, [s3path+"/"])
        dc.add_git_commit(src=__file__)
        try:
            dc.commit()
        except DVSException as e:
            logging.error("DVSException: %s",str(e))



if __name__ == "__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

    dc = get_dvs()
    description = "Combine multiple files on Amazon S3 to the same file."
    if dc is not None:
        description += " Reports changes to DVS"
    else:
        description += " (DVS disabled)"

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter,
                            description=description)
    parser.add_argument("--demand_success", action='store_true',
                        help="require that a _SUCCESS part exists; fail if it does not")
    parser.add_argument("--suffix", help="Add this suffix to the S3 name created", default='')
    parser.add_argument("--verbose", help="Print info as it happens", action='store_true')
    parser.add_argument("--multipart", help="Use multipart-upload feature", action='store_true')
    parser.add_argument("--message", help="A message (commit comment) for the DVS")
    parser.add_argument("--debug", help="Enable debugging", action='store_true')
    parser.add_argument("--use_dvs", help="Use DVS", action='store_true')
    parser.add_argument("prefix", help="Amazon S3 prefix, include s3://")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    s3cat(args.prefix, demand_success=args.demand_success, suffix=args.suffix, verbose=args.verbose,
          message=args.message, debug=args.debug, use_dvs=args.use_dvs)
