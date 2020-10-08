#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Blob management in S3"""
import logging
import os
import stat
import boto3
import ZODB.blob
from ZODB import POSException
from relstorage.blobhelper import BlobHelper, _accessed, _lock_blob

logger = logging.getLogger("relstorage")


def s3_blob_filename(oid, serial):
    return oid.encode('hex') + serial.encode('hex') + '.blob'


class S3BlobHelper(BlobHelper):
    """Blob storage in S3 for RelStorage"""
    def __init__(self, options, adapter, fshelper=None, cache_checker=None):
        assert options.shared_blob_dir, "shared_blob_dir option must be \
            enabled in order to use S3"

        super(S3BlobHelper, self).__init__(options, adapter, fshelper,
                                           cache_checker)
        session = boto3.Session(
            aws_access_key_id=options.aws_access_key_id,
            aws_secret_access_key=options.aws_secret_access_key,
        )
        self.bucket = session.resource('s3',
                                       endpoint_url=options.endpoint_url,
                                       region_name=options.region_name).Bucket(
                                           options.bucket_name)

    def new_instance(self, adapter):
        return S3BlobHelper(options=self.options,
                            adapter=adapter,
                            fshelper=self.fshelper,
                            cache_checker=self.cache_checker)

    def download_blob(self, cursor, oid, serial, filename):
        super(S3BlobHelper, self).download_blob(cursor, oid, serial, filename)
        if os.path.exists(filename):
            return
        key = s3_blob_filename(oid, serial)

        # Confirm blob cache directory is locked for writes
        cache_filename = self.fshelper.getBlobFilename(oid, serial)
        lock_filename = os.path.join(os.path.dirname(cache_filename), '.lock')
        assert os.path.exists(lock_filename)

        # Download
        self.bucket.download_file(key, cache_filename)
        os.chmod(cache_filename, stat.S_IREAD)

    def loadBlob(self, cursor, oid, serial):
        blob_filename = self.fshelper.getBlobFilename(oid, serial)

        if os.path.exists(blob_filename):
            return _accessed(blob_filename)

        # First, we'll create the directory for this oid, if it doesn't exist.
        self.fshelper.getPathForOID(oid, create=True)

        # OK, it's not here and we (or someone) needs to get it.  We
        # want to avoid getting it multiple times.  We want to avoid
        # getting it multiple times even accross separate client
        # processes on the same machine. We'll use file locking.

        lock = _lock_blob(blob_filename)
        try:
            # We got the lock, so it's our job to download it.  First,
            # we'll double check that someone didn't download it while we
            # were getting the lock:

            if os.path.exists(blob_filename):
                return _accessed(blob_filename)

            self.download_blob(cursor, oid, serial, blob_filename)

            if os.path.exists(blob_filename):
                return _accessed(blob_filename)

            raise POSException.POSKeyError("No blob file", oid, serial)

        finally:
            lock.close()

    def openCommittedBlobFile(self, cursor, oid, serial, blob=None):
        blob_filename = self.loadBlob(cursor, oid, serial)
        try:
            if blob is None:
                return open(blob_filename, 'rb')
            else:
                return ZODB.blob.BlobFile(blob_filename, 'r', blob)
        except IOError:
            # The file got removed while we were opening.
            # Fall through and try again with the protection of the lock.
            pass

        lock = _lock_blob(blob_filename)
        try:
            blob_filename = self.fshelper.getBlobFilename(oid, serial)
            if not os.path.exists(blob_filename):
                self.download_blob(cursor, oid, serial, blob_filename)
                if not os.path.exists(blob_filename):
                    raise POSException.POSKeyError("No blob file", oid, serial)

            _accessed(blob_filename)
            if blob is None:
                return open(blob_filename, 'rb')
            else:
                return ZODB.blob.BlobFile(blob_filename, 'r', blob)
        finally:
            lock.close()
