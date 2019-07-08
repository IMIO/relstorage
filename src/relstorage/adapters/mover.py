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
"""IObjectMover implementation.
"""
from __future__ import absolute_import

from hashlib import md5
from abc import abstractmethod

from perfmetrics import Metric
from zope.interface import implementer
from ZODB.POSException import Unsupported

from .._compat import OID_TID_MAP_TYPE
from ._util import noop_when_history_free
from ._util import query_property as _query_property
from .._compat import ABC
from .batch import RowBatcher
from .interfaces import IObjectMover

metricmethod_sampled = Metric(method=True, rate=0.1)

@implementer(IObjectMover)
class AbstractObjectMover(ABC):

    def __init__(self, database_driver, options, runner=None,
                 version_detector=None,
                 batcher_factory=RowBatcher):
        """
        :param database_driver: The `IDBDriver` in use.
        """
        self.driver = database_driver
        self.keep_history = options.keep_history
        self.blob_chunk_size = options.blob_chunk_size
        self.runner = runner

        self.version_detector = version_detector
        self.make_batcher = batcher_factory

    @noop_when_history_free
    def _compute_md5sum(self, data):
        if data is None:
            return None
        return md5(data).hexdigest()

    _load_current_queries = (
        """
        SELECT state, tid
        FROM current_object
        JOIN object_state USING(zoid, tid)
        WHERE zoid = %s
        """,
        """
        SELECT state, tid
        FROM object_state
        WHERE zoid = %s
        """)

    _load_current_query = _query_property('_load_current')

    @metricmethod_sampled
    def load_current(self, cursor, oid):
        """Returns the current pickle and integer tid for an object.

        oid is an integer.  Returns (None, None) if object does not exist.
        """
        stmt = self._load_current_query

        cursor.execute(stmt, (oid,))
        # Note that we cannot rely on cursor.rowcount being
        # a valid indicator. The DB-API doesn't require it, and
        # some implementations, like MySQL Connector/Python are
        # unbuffered by default and can't provide it.
        row = cursor.fetchone()
        if row:
            state, tid = row
            state = self.driver.binary_column_as_state_type(state)
            # If it's None, the object's creation has been
            # undone.
            return state, tid

        return None, None

    _load_currents_queries = (
        (('zoid', 'state', 'tid'), 'current_object JOIN object_state USING(zoid, tid)', 'zoid'),
        (('zoid', 'state', 'tid'), 'object_state', 'zoid'),
    )

    _load_currents_query = _query_property('_load_currents')

    @metricmethod_sampled
    def load_currents(self, cursor, oids):
        """Returns the current {oid: tid} for specified object ids."""
        columns, table, filter_column = self._load_currents_query
        binary_column_as_state_type = self.driver.binary_column_as_state_type
        batcher = self.make_batcher(cursor, row_limit=1000)
        rows = batcher.select_from(columns, table, **{filter_column: oids})
        for row in rows:
            oid, state, tid = row
            yield oid, binary_column_as_state_type(state), tid

    _load_revision_query = """
        SELECT state
        FROM object_state
        WHERE zoid = %s
            AND tid = %s
        """

    @metricmethod_sampled
    def load_revision(self, cursor, oid, tid):
        """Returns the pickle for an object on a particular transaction.

        Returns None if no such state exists.
        """
        stmt = self._load_revision_query
        cursor.execute(stmt, (oid, tid))
        row = cursor.fetchone()
        if row:
            (state,) = row
            return self.driver.binary_column_as_state_type(state)
        return None

    _exists_queries = (
        "SELECT 1 FROM current_object WHERE zoid = %s",
        "SELECT 1 FROM object_state WHERE zoid = %s"
    )

    _exists_query = _query_property('_exists')

    @metricmethod_sampled
    def exists(self, cursor, oid):
        """Returns a true value if the given object exists."""
        stmt = self._exists_query
        cursor.execute(stmt, (oid,))
        row = cursor.fetchone()
        return row

    @metricmethod_sampled
    def load_before(self, cursor, oid, tid):
        """Returns the pickle and tid of an object before transaction tid.

        Returns (None, None) if no earlier state exists.
        """
        stmt = """
        SELECT state, tid
        FROM object_state
        WHERE zoid = %s
            AND tid < %s
        ORDER BY tid DESC
        LIMIT 1
        """
        cursor.execute(stmt, (oid, tid))
        row = cursor.fetchone()
        if row:
            state, tid = row
            state = self.driver.binary_column_as_state_type(state)
            # None in state means The object's creation has been undone
            return state, tid

        return None, None


    @metricmethod_sampled
    def get_object_tid_after(self, cursor, oid, tid):
        """Returns the tid of the next change after an object revision.

        Returns None if no later state exists.
        """
        stmt = """
        SELECT tid
        FROM object_state
        WHERE zoid = %s
            AND tid > %s
        ORDER BY tid
        LIMIT 1
        """
        cursor.execute(stmt, (oid, tid))
        row = cursor.fetchone()
        if row:
            return row[0]

    _current_object_tids_queries = (
        (('zoid', 'tid'), 'current_object', 'zoid'),
        (('zoid', 'tid'), 'object_state', 'zoid'),
    )

    _current_object_tids_query = _query_property('_current_object_tids')

    _current_object_tids_map_type = OID_TID_MAP_TYPE

    @metricmethod_sampled
    def current_object_tids(self, cursor, oids):
        """Returns the current {oid: tid} for specified object ids."""
        res = self._current_object_tids_map_type()
        columns, table, filter_column = self._current_object_tids_query
        batcher = self.make_batcher(cursor, row_limit=1000)
        rows = batcher.select_from(columns, table, **{filter_column: oids})
        res = self._current_object_tids_map_type(list(rows))

        return res


    #: A sequence of *names* of attributes on this object that are statements to be
    #: executed by ``on_store_opened`` when ``restart`` is False.
    on_store_opened_statement_names = ()

    def on_store_opened(self, cursor, restart=False):
        if restart:
            return

        if not restart:
            for stmt_name in self.on_store_opened_statement_names:
                try:
                    prep = getattr(self, stmt_name)
                except Unsupported:
                    # Must not be needed
                    pass
                else:
                    cursor.execute(prep)


    #: A sequence of *names* of attributes on this object that are statements to be
    #: executed by ``on_store_opened`` when ``restart`` is False.
    on_load_opened_statement_names = ()

    def on_load_opened(self, cursor, restart=False):
        if not restart:
            for stmt_name in self.on_load_opened_statement_names:
                cursor.execute(getattr(self, stmt_name))

    # The _generic methods allow for UPSERTs, at least on MySQL
    # and PostgreSQL. Previously, MySQL used `command='REPLACE'`
    # for an UPSERT; now it uses a suffix 'ON DUPLICATE KEY UPDATE ...'.
    # PostgreSQL uses a suffix 'ON CONFLICT (...) UPDATE ...'.

    def _generic_store_temp(self, batcher, oid, prev_tid, data,
                            command='INSERT', suffix=''):
        md5sum = self._compute_md5sum(data)
        # TODO: Now that we guarantee not to feed duplicates here, drop
        # the conflict handling.
        if command == 'INSERT' and not suffix:
            batcher.delete_from('temp_store', zoid=oid)
        batcher.insert_into(
            "temp_store (zoid, prev_tid, md5, state)",
            "%s, %s, %s, %s",
            (oid, prev_tid, md5sum, self.driver.Binary(data)),
            rowkey=oid,
            size=len(data),
            command=command,
            suffix=suffix
        )

    @abstractmethod
    def store_temp(self, cursor, batcher, oid, prev_tid, data):
        raise NotImplementedError()

    @metricmethod_sampled
    def store_temps(self, cursor, state_oid_tid_iter):
        batcher = self.make_batcher(cursor) # Default row limit
        store_temp = self.store_temp
        for data, oid_int, tid_int in state_oid_tid_iter:
            store_temp(cursor, batcher, oid_int, tid_int, data)
        batcher.flush()

    @metricmethod_sampled
    def _generic_restore(self, batcher, oid, tid, data,
                         command='INSERT', suffix=''):
        """Store an object directly, without conflict detection.

        Used for copying transactions into this database.
        """
        md5sum = self._compute_md5sum(data)

        if data is not None:
            encoded = self.driver.Binary(data)
            size = len(data)
        else:
            encoded = None
            size = 0

        if self.keep_history:
            if command == 'INSERT' and not suffix:
                batcher.delete_from("object_state", zoid=oid, tid=tid)
            row_schema = """
                %s, %s,
                COALESCE((SELECT tid FROM current_object WHERE zoid = %s), 0),
                %s, %s, %s
            """
            batcher.insert_into(
                "object_state (zoid, tid, prev_tid, md5, state_size, state)",
                row_schema,
                (oid, tid, oid, md5sum, size, encoded),
                rowkey=(oid, tid),
                size=size,
                command=command,
                suffix=suffix
            )
        elif data:
            if command == 'INSERT' and not suffix:
                batcher.delete_from('object_state', zoid=oid)
            batcher.insert_into(
                "object_state (zoid, tid, state_size, state)",
                "%s, %s, %s, %s",
                (oid, tid, size, encoded),
                rowkey=oid,
                size=size,
                command=command,
                suffix=suffix
            )
        else:
            batcher.delete_from('object_state', zoid=oid)

    def restore(self, cursor, batcher, oid, tid, data):
        raise NotImplementedError()

    # careful with USING clause in a join: Oracle doesn't allow such
    # columns to have a prefix.
    _detect_conflict_queries = (
        """
        SELECT zoid, current_object.tid, temp_store.prev_tid
        FROM temp_store
        JOIN current_object USING (zoid)
        WHERE temp_store.prev_tid != current_object.tid
        ORDER BY zoid
        """,
        """
        SELECT zoid, object_state.tid, temp_store.prev_tid
        FROM temp_store
        JOIN object_state USING (zoid)
        WHERE temp_store.prev_tid != object_state.tid
        ORDER BY zoid
        """
    )

    _detect_conflict_query = _query_property('_detect_conflict')

    @metricmethod_sampled
    def detect_conflict(self, cursor):
        # TODO: We should return the committed state so it can be
        # passed to tryToResolveConflict, saving extra queries.
        # OTOH, using extra memory.
        stmt = self._detect_conflict_query
        cursor.execute(stmt)
        rows = cursor.fetchall()
        return rows

    @metricmethod_sampled
    def replace_temp(self, cursor, oid, prev_tid, data):
        """Replace an object in the temporary table.

        This happens after conflict resolution.
        """
        md5sum = self._compute_md5sum(data)

        stmt = """
        UPDATE temp_store SET
            prev_tid = %s,
            md5 = %s,
            state = %s
        WHERE zoid = %s
        """
        cursor.execute(stmt, (prev_tid, md5sum, self.driver.Binary(data), oid))

    # Subclasses may override any of these queries if there is a
    # more optimal form.

    _move_from_temp_hp_insert_query = """
    INSERT INTO object_state
      (zoid, tid, prev_tid, md5, state_size, state)
    SELECT zoid, %s, prev_tid, md5,
      COALESCE(LENGTH(state), 0), state
      FROM temp_store
      ORDER BY zoid
    """

    _move_from_temp_hf_delete_query = """
    DELETE FROM object_state
    WHERE zoid IN (SELECT zoid FROM temp_store)
    """

    _move_from_temp_hf_insert_query = """
    INSERT INTO object_state (zoid, tid, state_size, state)
        SELECT zoid, %s, COALESCE(LENGTH(state), 0), state
        FROM temp_store
        ORDER BY zoid
    """

    _move_from_temp_copy_blob_query = """
    INSERT INTO blob_chunk (zoid, tid, chunk_num, chunk)
    SELECT zoid, %s, chunk_num, chunk
    FROM temp_blob_chunk
    """

    _move_from_temp_hf_delete_blob_chunk_query = """
    DELETE FROM blob_chunk
    WHERE zoid IN (SELECT zoid FROM temp_store)
    """


    def _move_from_temp_object_state(self, cursor, tid):
        """
        Called for history-free databases.

        Should replace all entries in object_state with the same zoid
        from temp_store.

        This implementation is in two steps, first deleting from
        ``object_state`` with :attr:`_move_from_temp_hf_delete_query`,
        and then copying from ``temp_store`` using
        :attr:`_move_from_temp_hf_insert_query`.

        If a subclass can do this in a single step with an ``UPSERT``,
        it should set :attr:`_move_from_temp_hf_delete_query` to a
        false value.

        Recall that the queries that touch ``current_object`` and
        ``object_state`` need to be certain the order they use (by
        ``zoid``) to avoid deadlocks.

        Blobs are handled separately.
        """
        stmt = self._move_from_temp_hf_delete_query
        if stmt:
            cursor.execute(stmt)

        stmt = self._move_from_temp_hf_insert_query
        cursor.execute(stmt, (tid,))


    @metricmethod_sampled
    def move_from_temp(self, cursor, tid, txn_has_blobs):
        """
        Move the temporarily stored objects to permanent storage.
        """
        if self.keep_history:
            stmt = self._move_from_temp_hp_insert_query
            __traceback_info__ = stmt
            cursor.execute(stmt, (tid,))
        else:
            self._move_from_temp_object_state(cursor, tid)

            if txn_has_blobs:
                # If we can require storages to have an UPSERT (mysql and
                # postgres do), then we can remove the DELETE.
                stmt = self._move_from_temp_hf_delete_blob_chunk_query
                cursor.execute(stmt)

        # TODO: Make this an UPSERT for history free storages.
        # This would obviate the need for the above delete query.
        if txn_has_blobs:
            stmt = self._move_from_temp_copy_blob_query
            __traceabck_info__ = stmt
            cursor.execute(stmt, (tid,))


    # Insert and update current objects. The trivial
    # implementation does a two-part query; if you
    # have an UPSERT statement that can do it in one query,
    # then put that in `_update_current_insert_query`
    # and set `_update_current_update_query` to None.
    # Note that to avoid deadlocks, it is incredibly important
    # to order the updates in OID order.
    _update_current_insert_query = """
        INSERT INTO current_object (zoid, tid)
        SELECT zoid, tid FROM object_state
        WHERE tid = %s
            AND prev_tid = 0
    """

    _update_current_update_query = """
        UPDATE current_object
        SET tid = %s
        WHERE zoid IN (
            SELECT zoid FROM object_state
            WHERE tid = %s
                AND prev_tid != 0
            ORDER BY zoid
        )
    """

    @noop_when_history_free
    @metricmethod_sampled
    def update_current(self, cursor, tid):
        """
        Update the current object pointers.

        tid is the integer tid of the transaction being committed.
        """
        stmt = self._update_current_insert_query
        cursor.execute(stmt, (tid,))

        if self._update_current_update_query:
            stmt = self._update_current_update_query
            cursor.execute(stmt, (tid, tid))

    @metricmethod_sampled
    def download_blob(self, cursor, oid, tid, filename):
        """Download a blob into a file."""
        raise NotImplementedError()

    def upload_blob(self, cursor, oid, tid, filename):
        """Upload a blob from a file.

        If serial is None, upload to the temporary table.
        """
        raise NotImplementedError()