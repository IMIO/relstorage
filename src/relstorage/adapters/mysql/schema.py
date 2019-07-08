##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
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
"""
Database schema installers
"""
from __future__ import absolute_import

from ZODB.POSException import StorageError
from zope.interface import implementer

from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller

logger = __import__('logging').getLogger(__name__)

@implementer(ISchemaInstaller)
class MySQLSchemaInstaller(AbstractSchemaInstaller):

    database_type = 'mysql'

    # The names of tables that in the past were explicitly declared as
    # MyISAM but which should now be InnoDB to work with transactions.
    # TODO: Add a migration check for this.
    tables_that_used_to_be_myisam_should_be_innodb = (
        # new_oid, # (This needs to be done, but we're not quite there yet)
        'object_ref',
        'object_refs_added',
        'pack_object',
        'pack_state',
        'pack_state_tid',
    )

    def get_database_name(self, cursor):
        cursor.execute("SELECT DATABASE()")
        for (name,) in cursor:
            return self._metadata_to_native_str(name)

    def list_tables(self, cursor):
        cursor.execute("SHOW TABLES")
        return [self._metadata_to_native_str(name)
                for (name,) in cursor.fetchall()]

    def list_sequences(self, cursor):
        return []

    def check_compatibility(self, cursor, tables):
        super(MySQLSchemaInstaller, self).check_compatibility(cursor, tables)
        # TODO: Check more tables, like `transaction`
        stmt = "SHOW TABLE STATUS LIKE 'object_state'"
        cursor.execute(stmt)
        for row in self._rows_as_dicts(cursor):
            engine = self._metadata_to_native_str(row['engine'])
            if engine.lower() != 'innodb':
                raise StorageError(
                    "The object_state table must use the InnoDB "
                    "engine, but it is using the %s engine." % engine)

    def _create_pack_lock(self, cursor):
        return

    COLTYPE_BINARY_STRING = 'BLOB'
    TRANSACTIONAL_TABLE_SUFFIX = 'ENGINE = InnoDB'

    # As usual, MySQL has a quirky implementation of this feature and we
    # have to re-specify *everything* about the column. MySQL 8 supports the
    # simple 'RENAME ... TO ...' syntax that everyone else does.
    _rename_transaction_empty_stmt = (
        "ALTER TABLE transaction CHANGE empty is_empty "
        "BOOLEAN NOT NULL DEFAULT FALSE"
    )


    def _create_new_oid(self, cursor):
        stmt = """
        CREATE TABLE new_oid (
            zoid        BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT
        ) ENGINE = MyISAM;
        """
        self.runner.run_script(cursor, stmt)


    def _create_object_state(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE object_state (
                zoid        BIGINT NOT NULL,
                tid         BIGINT NOT NULL REFERENCES transaction,
                            PRIMARY KEY (zoid, tid),
                            CHECK (tid > 0),
                prev_tid    BIGINT NOT NULL REFERENCES transaction,
                md5         CHAR(32) CHARACTER SET ascii,
                state_size  BIGINT NOT NULL,
                state       LONGBLOB
            ) ENGINE = InnoDB;
            CREATE INDEX object_state_tid ON object_state (tid);
            CREATE INDEX object_state_prev_tid ON object_state (prev_tid);
            """
        else:
            stmt = """
            CREATE TABLE object_state (
                zoid        BIGINT NOT NULL PRIMARY KEY,
                tid         BIGINT NOT NULL,
                            CHECK (tid > 0),
                state_size  BIGINT NOT NULL,
                state       LONGBLOB
            ) ENGINE = InnoDB;
            CREATE INDEX object_state_tid ON object_state (tid);
            """

        self.runner.run_script(cursor, stmt)

    def _create_blob_chunk(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE blob_chunk (
                zoid        BIGINT NOT NULL,
                tid         BIGINT NOT NULL,
                chunk_num   BIGINT NOT NULL,
                            PRIMARY KEY (zoid, tid, chunk_num),
                chunk       LONGBLOB NOT NULL
            ) ENGINE = InnoDB;
            CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid, tid);
            ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
                FOREIGN KEY (zoid, tid)
                REFERENCES object_state (zoid, tid)
                ON DELETE CASCADE;
            """
        else:
            stmt = """
            CREATE TABLE blob_chunk (
                zoid        BIGINT NOT NULL,
                chunk_num   BIGINT NOT NULL,
                            PRIMARY KEY (zoid, chunk_num),
                tid         BIGINT NOT NULL,
                chunk       LONGBLOB NOT NULL
            ) ENGINE = InnoDB;
            CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid);
            ALTER TABLE blob_chunk ADD CONSTRAINT blob_chunk_fk
                FOREIGN KEY (zoid)
                REFERENCES object_state (zoid)
                ON DELETE CASCADE;
            """

        self.runner.run_script(cursor, stmt)

    def _create_current_object(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE current_object (
                zoid        BIGINT NOT NULL PRIMARY KEY,
                tid         BIGINT NOT NULL,
                            FOREIGN KEY (zoid, tid)
                                REFERENCES object_state (zoid, tid)
            ) ENGINE = InnoDB;
            CREATE INDEX current_object_tid ON current_object (tid);
            """
            self.runner.run_script(cursor, stmt)

    def _create_object_ref(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE object_ref (
                zoid        BIGINT NOT NULL,
                tid         BIGINT NOT NULL,
                to_zoid     BIGINT NOT NULL,
                PRIMARY KEY (tid, zoid, to_zoid)
            ) ENGINE = InnoDB;
            """
        else:
            stmt = """
            CREATE TABLE object_ref (
                zoid        BIGINT NOT NULL,
                to_zoid     BIGINT NOT NULL,
                tid         BIGINT NOT NULL,
                PRIMARY KEY (zoid, to_zoid)
            ) ENGINE = InnoDB;
            """

        self.runner.run_script(cursor, stmt)

    def _create_object_refs_added(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE object_refs_added (
                tid         BIGINT NOT NULL PRIMARY KEY
            ) ENGINE = InnoDB;
            """
        else:
            stmt = """
            CREATE TABLE object_refs_added (
                zoid        BIGINT NOT NULL PRIMARY KEY,
                tid         BIGINT NOT NULL
            ) ENGINE = InnoDB;
            """
        self.runner.run_script(cursor, stmt)

    def _create_pack_object(self, cursor):
        stmt = """
        CREATE TABLE pack_object (
            zoid        BIGINT NOT NULL PRIMARY KEY,
            keep        BOOLEAN NOT NULL,
            keep_tid    BIGINT NOT NULL,
            visited     BOOLEAN NOT NULL DEFAULT FALSE
        ) ENGINE = InnoDB;
        CREATE INDEX pack_object_keep_zoid ON pack_object (keep, zoid);
        """
        self.runner.run_script(cursor, stmt)

    def _create_pack_state(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE pack_state (
                tid         BIGINT NOT NULL,
                zoid        BIGINT NOT NULL,
                PRIMARY KEY (tid, zoid)
            ) ENGINE = InnoDB;
            """
            self.runner.run_script(cursor, stmt)

    def _create_pack_state_tid(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE pack_state_tid (
                tid         BIGINT NOT NULL PRIMARY KEY
            ) ENGINE = InnoDB;
            """
            self.runner.run_script(cursor, stmt)

    # Temp tables are created in a session-by-session basis
    def _create_temp_store(self, _cursor):
        return

    def _create_temp_blob_chunk(self, _cursor):
        return

    def _create_temp_pack_visit(self, _cursor):
        return

    def _create_temp_undo(self, _cursor):
        return

    def _reset_oid(self, cursor):
        stmt = "TRUNCATE new_oid;"
        self.runner.run_script(cursor, stmt)

    # We can't TRUNCATE tables that have foreign-key relationships
    # with other tables, but we can drop them. This has to be followed up by
    # creating them again.
    _zap_all_tbl_stmt = 'DROP TABLE %s'

    def _after_zap_all_tables(self, cursor, slow=False):
        if not slow:
            logger.debug("Creating tables after drop")
            self.create(cursor)
            logger.debug("Done creating tables after drop")
        else:
            super(MySQLSchemaInstaller, self)._after_zap_all_tables(cursor, slow)
