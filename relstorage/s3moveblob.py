#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script to archive blobs from local blobstorage to S3.

This is meant to be run periodically to transfer blobs
from the ZEO server's local blobstorage to S3.
"""
import argparse
import logging
import os
import time
import magic

import boto3
from relstorage.s3blobhelper import s3_blob_filename
import ZODB.blob

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='Archive blobs to S3')
parser.add_argument('blob_dir', help='Path to local blobstorage directory')
parser.add_argument('bucket_name', help='S3 bucket name')
parser.add_argument('-e', '--endpoint-url', help='Endpoint URL')
parser.add_argument('-r', '--region', help='Id of the region')
parser.add_argument(
    '-a',
    '--age',
    type=int,
    default=1,
    help='Only archive blobs created more than this many days ago. \
 (Default: 1)')
parser.add_argument(
    '-s',
    '--size',
    type=int,
    default=0,
    help='Only archive blobs more than this many bytes in size. (Default: 0)')
parser.add_argument('-d',
                    '--destroy',
                    action='store_true',
                    dest='exterminate',
                    help='Destroy local file after archiving?')
parser.add_argument(
    '-b',
    '--backup',
    action='store_true',
    help='Move destroyed files to add .bak extension rather than deleting')


def main():
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()

    aws_access_key_id = os.environ.get('aws_access_key_id')
    aws_secret_access_key = os.environ.get('aws_secret_access_key')
    endpoint = args.endpoint_url or None
    region = args.region or None
    bucket = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    ).resource('s3', endpoint_url=endpoint,
               region_name=region).Bucket(args.bucket_name)

    # Get list of files that already exist in the bucket
    existing = set(obj.key for obj in bucket.objects.all())

    fshelper = ZODB.blob.FilesystemHelper(args.blob_dir)
    count = 0
    total_size = 0

    # Iterate through paths in blobstorage
    for oid, path in fshelper.listOIDs():
        for filename in os.listdir(path):
            filepath = os.path.join(path, filename)
            st = os.stat(filepath)

            # Skip new files
            age = (time.time() - st.st_ctime) / 86400
            if age < args.age:
                logger.debug(
                    'Skipping {} because it is too new.'.format(filepath))
                continue

            # Skip small files
            size = st.st_size
            if size < args.size:
                logger.debug(
                    'Skipping {} because it is too small.'.format(filepath))
                continue

            # Determine the S3 object key/filename
            whatever, serial = fshelper.splitBlobFilename(filepath)
            if serial is None:
                continue
            blob_filename = s3_blob_filename(oid, serial)

            # Upload files that aren't in the bucket yet
            if blob_filename not in existing:
                # There's a hole in the bucket, dear Liza, dear Liza...
                extra_args = {
                    'ContentType': magic.from_file(filepath, mime=True)
                }
                bucket.upload_file(filepath,
                                   blob_filename,
                                   ExtraArgs=extra_args)

                # Log
                count += 1
                total_size += size
                logger.info('Uploaded {} to S3 ({}).'.format(
                    blob_filename, size))

            # Remove local file
            if args.exterminate:
                if args.backup:
                    os.rename(filepath, '{}.bak'.format(filepath))
                    logger.info('Moved {} to {}.bak'.format(
                        filepath, filepath))
                else:
                    os.remove(filepath)
                    logger.info('Deleted {}'.format(filepath))

    if count:
        logger.info('Done uploading {} files to S3. ({})'.format(
            count, total_size))
    else:
        logger.info("Found no new files to upload to S3.")
