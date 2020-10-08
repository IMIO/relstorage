##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""ZConfig directive implementations for binding RelStorage to Zope"""

from ZODB.config import BaseConfig

from relstorage.options import Options
from relstorage.storage import RelStorage
from relstorage.adapters.replica import ReplicaSelector

import os


class RelStorageFactory(BaseConfig):
    """Open a storage configured via ZConfig"""
    def open(self):
        config = self.config
        options = Options()
        for key in options.__dict__.keys():
            value = getattr(config, key, None)
            if value is not None:
                setattr(options, key, value)
        adapter = config.adapter.create(options)
        if config.blobhelper:
            blobhelper = config.blobhelper.create(options, adapter)
        else:
            blobhelper = None
        return RelStorage(adapter,
                          name=config.name,
                          options=options,
                          blobhelper=blobhelper)


class S3BlobHelperAdapterFactory(BaseConfig):
    def create(self, options, adapter):
        from s3blobhelper import S3BlobHelper
        options.bucket_name = self.config.bucket_name
        options.endpoint_url = self.config.endpoint_url
        options.region_name = self.config.region_name
        options.aws_access_key_id = os.environ.get('aws_access_key_id')
        options.aws_secret_access_key = os.environ.get('aws_secret_access_key')
        return S3BlobHelper(options, adapter)


class PostgreSQLAdapterFactory(BaseConfig):
    def create(self, options):
        from adapters.postgresql import PostgreSQLAdapter
        return PostgreSQLAdapter(
            dsn=self.config.dsn,
            options=options,
            )


class OracleAdapterFactory(BaseConfig):
    def create(self, options):
        from adapters.oracle import OracleAdapter
        config = self.config
        return OracleAdapter(
            user=config.user,
            password=config.password,
            dsn=config.dsn,
            options=options,
            )


class MySQLAdapterFactory(BaseConfig):
    def create(self, options):
        from adapters.mysql import MySQLAdapter
        params = {}
        for key in self.config.getSectionAttributes():
            value = getattr(self.config, key)
            if value is not None:
                params[key] = value
        return MySQLAdapter(options=options, **params)

