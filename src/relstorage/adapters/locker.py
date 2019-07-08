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
Locker implementations.
"""

from __future__ import absolute_import
from __future__ import print_function

import abc
import sys
import six

from perfmetrics import metricmethod

from relstorage._compat import ABC
from relstorage._util import consume
from relstorage._util import Lazy

from ._util import query_property as _query_property
from ._util import DatabaseHelpersMixin
from .interfaces import UnableToAcquireCommitLockError

logger = __import__('logging').getLogger(__name__)

class AbstractLocker(DatabaseHelpersMixin,
                     ABC):

    def __init__(self, options, driver, batcher_factory):
        self.keep_history = options.keep_history
        self.commit_lock_timeout = options.commit_lock_timeout
        self.commit_lock_id = options.commit_lock_id
        self.driver = driver
        self.lock_exceptions = driver.lock_exceptions
        self.illegal_operation_exceptions = driver.illegal_operation_exceptions
        self.make_batcher = batcher_factory

    def on_store_opened(self, cursor, restart=False):
        """
        A callback that must be called when a store connection is
        opened or restarted.

        This implementation calls :meth:`_on_store_opened_set_row_lock_timeout`
        when the store connection is initially opened.
        """
        if not restart:
            self._on_store_opened_set_row_lock_timeout(cursor)

    def _on_store_opened_set_row_lock_timeout(self, cursor, restart=False):
        """
        Install a per-session row lock timeout.

        This should be set to the :attr:`commit_lock_timeout`.

        This applies to the locks taken by :meth:`lock_current_objects` during the
        ``tpc_vote`` phase of a transaction.
        """

    #: Set this to a false value if you don't support ``NOWAIT`` and we'll
    #: instead set the lock timeout to 0.
    _supports_row_lock_nowait = True

    def _set_row_lock_timeout(self, cursor, timeout):
        "Implement this to change the row lock timeout."

    def _set_row_lock_nowait(self, cursor): # pragma: no cover
        """
        If :attr:`_supports_row_lock_timeout` is not true, then this
        class will call this method when :meth:`hold_commit_lock` is
        called with a false value for *nowait*.

        This method in turn is implemented to call
        :meth:`_set_row_lock_timeout` with a *timeout* argument of 0.

        This method is deprecated, as the *nowait* parameter is deprecated.

        ..note::

            This class does not attempt to revert this change, leaving
            the connection in a 0 timeout.

            Previously, we only set *nowait* to false during packing, and only
            for that one connection. So it didn't matter that it wasn't rolled back.

            Now, we don't use *nowait* at all.

            We could automatically make this change local to the transaction
            on PostgreSQL with ``SET LOCAL``, and on MySQL with the
            optimizer hint ``SET_VAR()`` in the lock statement --- but only in 8+.
        """
        self._set_row_lock_timeout(cursor, 0)

    _lock_current_clause = 'FOR UPDATE'

    #: These double as the query to get OIDs we'd like to lock, but
    #: do not actually lock them.
    _get_current_objects_queries = (
        # If we also include the objects being added,
        # mysql takes out gap locks, and we can deadlock?
        # TODO: Confirm.
        ("""
        SELECT zoid
        FROM current_object
        WHERE zoid IN (
            SELECT zoid
            FROM temp_store
        )
        """, 'current_object'),
        ("""
        SELECT zoid
        FROM object_state
        WHERE zoid IN (
            SELECT zoid
            FROM temp_store
        )
        """, 'object_state'),
    )

    _get_current_objects_query = _query_property('_get_current_objects')

    @Lazy
    def _lock_current_objects_query(self):
        return '{select} ORDER BY ZOID {lock}'.format(
            select=self._get_current_objects_query[0],
            lock=self._lock_current_clause
        )

    def lock_current_objects(self, cursor, current_oids):
        # We need to be sure to take the locks in a deterministic
        # order; the easiest way to do that is to order them by OID.
        # But we have two separate sets of OIDs we need to lock: the
        # ones we're finding the current data for, and the ones that
        # we're going to check for conflicts. The ones we're checking
        # for conflicts are already in the database in ``temp_store``
        # (and partly in the storage cache's temporary storage and/or
        # the row batcher); the current oids are only in memory.
        #
        # So we have a few choices: either put the current oids into
        # a database table and do a UNION query with temp_store,
        # or pull the temp_store data into memory, union it with
        # current_oids and issue a single big query.
        #
        # Our strategy could even vary depending on the size of
        # *current_oids*; in the usual case, it will be small or
        # empty, and an in-database big UNION query is probably
        # workable (in the empty case, we can and do elide this part
        # altogether)


        # In history free mode, *table* will be `object_state`, which
        # has ZOID as its primary key. In history preserving mode,
        # *table* will be `current_object`, where ZOID is also the primary
        # key (and `object_state` is immutable; that's why we don't need
        # to take any locks there --- conflict resolution will always be able
        # to find the desired state without fear of it changing).

        # MySQL 8 allows ``NOWAIT`` and ``SKIP LOCKED`` on ``FOR
        # UPDATE`` or ``FOR SHARE`` clauses; earlier versions do not
        # have that. PostgreSQL allows both.

        # In all databases, the locks we get depend on the indexing.
        # We must be searching on the primary key to get the smallest,
        # most specific row locks. In some databases, rows are only
        # locked when they are returned by the cursor, so we must
        # consume all the rows.

        if current_oids:
            stmt, table = self._get_current_objects_query
            cursor.execute(stmt)

            oids_being_updated = {row[0] for row in cursor}

            oids_to_lock = oids_being_updated | set(current_oids)
            oids_to_lock = sorted(oids_to_lock)

            batcher = self.make_batcher(cursor, row_limit=1000)

            rows = batcher.select_from(
                ('zoid',), table,
                suffix='  %s ' % self._lock_current_clause,
                **{'zoid': oids_to_lock}
            )
        else:
            stmt = self._lock_current_objects_query
            cursor.execute(stmt)
            rows = cursor

        consume(rows)

    _commit_lock_queries = (
        # MySQL allows aggregates in the top level to use FOR UPDATE,
        # but PostgreSQL does not, so we have to use the second form.
        #
        # 'SELECT MAX(tid) FROM transaction FOR UPDATE',
        # 'SELECT tid FROM transaction WHERE tid = (SELECT MAX(tid) FROM transaction)  FOR UPDATE',

        # Note that using transaction in history-preserving databases
        # can still lead to deadlock in older versions of MySQL (test
        # checkPackWhileWriting), and the above lock statement can
        # lead to duplicate transaction ids being inserted on older
        # versions (5.7.12, PyMySQL:
        # https://ci.appveyor.com/project/jamadden/relstorage/builds/25748619/job/cyio3w54uqi026lr#L923).
        # So both HF and HP use an artificial lock row.
        #
        # TODO: Figure out exactly the best way to lock just the rows
        # in the transaction table we care about that works
        # everywhere, or a better way to choose the next TID.
        # gap/intention locks might be a clue.

        'SELECT tid FROM commit_row_lock FOR UPDATE',
        'SELECT tid FROM commit_row_lock FOR UPDATE'
    )

    _commit_lock_query = _query_property('_commit_lock')

    _commit_lock_nowait_queries = (
        _commit_lock_queries[0] + ' NOWAIT',
        _commit_lock_queries[1] + ' NOWAIT',
    )

    _commit_lock_nowait_query = _query_property('_commit_lock_nowait')


    @metricmethod
    def hold_commit_lock(self, cursor, ensure_current=False, nowait=False):
        # pylint:disable=unused-argument
        lock_stmt = self._commit_lock_query
        if nowait: # pragma: no cover
            if self._supports_row_lock_nowait:
                lock_stmt = self._commit_lock_nowait_query
            else:
                self._set_row_lock_nowait(cursor)
        __traceback_info__ = lock_stmt
        try:
            cursor.execute(lock_stmt)
            rows = cursor.fetchall()
            if not rows or not rows[0]:
                raise UnableToAcquireCommitLockError("No row returned from commit_row_lock")
        except self.illegal_operation_exceptions as e:
            # Bug in our code.
            raise
        except self.lock_exceptions as e:
            if nowait:
                return False

            try:
                debug_info = self._get_commit_lock_debug_info(cursor)
            except Exception as nested: # pylint:disable=broad-except
                logger.exception("Failed to get lock debug info")
                debug_info = "%r(%r)" % (type(nested), nested)
            __traceback_info__ = lock_stmt, debug_info
            if debug_info:
                logger.debug("Failed to acquire commit lock:\n%s", debug_info)
            message = "Acquiring a commit lock failed: %s%s" % (
                e,
                '\n' + debug_info if debug_info else ''
            )
            six.reraise(
                UnableToAcquireCommitLockError,
                UnableToAcquireCommitLockError(message),
                sys.exc_info()[2])
        return True

    def _get_commit_lock_debug_info(self, cursor): # pylint:disable=unused-argument
        """
        Subclasses can implement this to return a string
        that will be added to the exception message when a commit lock cannot
        be acquired. For example, it might list other connections that
        have conflicting locks.
        """
        return ''

    @abc.abstractmethod
    def release_commit_lock(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def hold_pack_lock(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def release_pack_lock(self, cursor):
        raise NotImplementedError()
