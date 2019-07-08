##############################################################################
#
# Copyright (c) 2017 Zope Foundation and Contributors.
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
"Internal helper utilities."

from collections import namedtuple
from functools import partial
from functools import update_wrapper
from functools import wraps

from .._compat import intern

from .._util import Lazy

def query_property(base_name,
                   extension='',
                   formatted=False):
    """
    Defines a property that adapts to preserving or dropping history.

    To use, define a property ending in `_queries` that is a
    two-tuple, where the preserving query comes first and the dropping
    query comes second. This indirection lets subclasses override
    these queries.

    Then define a property, passing the base name (without _queries)
    to this function.

    The correct query will be lazily picked at runtime. The instance
    must have the ``keep_history`` attribute.

    If the chosen query is an exception instance or class, it will be raised
    instead of returned. This allows defining a query that is only
    supported in one of the two modes. Usually, this exception should be
    :class:`ZODB.POSException.Unsupported`.

    :keyword str extension: This string will be appended to whatever
        query is chosen before it is formatted and before it is returned.
    :keyword bool formatted: If True (*not* the default), then the
        chosen query will be formatted using the
    ``self.runner.script_vars``.
    """

    def prop(inst):
        queries = getattr(inst, base_name + '_queries')
        query = queries[0] if inst.keep_history else queries[1]
        if isinstance(query, Exception) or (
                isinstance(query, type)
                and issubclass(query, Exception)):
            raise query

        if extension:
            query = query + extension
        if formatted:
            query = intern(query % inst.runner.script_vars)

        return query

    prop.__doc__ = "Query for " + base_name

    return Lazy(prop, base_name + '_query')

formatted_query_property = partial(query_property, formatted=True)


def noop_when_history_free(meth):
    """
    Decorator for *meth* that causes it to do nothing when
    ``self.keep_history`` is False.

    *meth* must have no return value (returns None) when it is
    history free. When history is preserved it can return anything.

    This requires a bit more memory to use the instance dict, but at
    runtime it has minimal time overhead (after the first call).
    """

    # Python 3.4 (via timeit)
    # calling a trivial method ('def t(self, arg): return arg') takes 118ns
    # calling a method that does 'if not self.keep_history: return; return arg'
    #   takes 142 ns
    # calling a functools.partial bound to self wrapped around t
    #   takes 298ns
    # calling a generic python function
    #     def wrap(self, *args, **kwargs):
    #       if not self.keep_history: return
    #       return self.t(*args, **kwargs)
    #   takes 429ns
    # So a partial function set into the __dict__ is the fastest way to
    # do this.

    meth_name = meth.__name__

    @wraps(meth)
    def no_op(*_args, **_kwargs):
        return

    @wraps(meth)
    def swizzler(self, *args, **kwargs):
        if not self.keep_history:
            setattr(self, meth_name, no_op)
        else:
            # NOTE: This creates a reference cycle
            bound = partial(meth, self)
            update_wrapper(bound, meth)
            if not hasattr(bound, '__wrapped__'):
                bound.__wrapped__ = meth
            setattr(self, meth_name, bound)

        return getattr(self, meth_name)(*args, **kwargs)

    if not hasattr(swizzler, '__wrapped__'):
        # Py2: this was added in 3.2
        swizzler.__wrapped__ = meth
        no_op.__wrapped__ = meth

    return swizzler


ResultDescription = namedtuple(
    'ResultDescription',
    # First two are mandatory, remaining five may be None
    # Example:
    # ('Name', 253, 17, 192, 192, 0, 0),
    ('name', 'type_code', 'display_size',
     'internal_size', 'precision', 'scale', 'null_ok'))


class DatabaseHelpersMixin(object):

    def _metadata_to_native_str(self, value):
        # Some drivers, in some configurations, notably older versions
        # of MySQLdb (mysqlclient) on Python 3 in 'NAMES binary' mode,
        # can return column names and the like as bytes when we want str.
        if not isinstance(value, str):
            value = value.decode('ascii')
        return value

    def _column_descriptions(self, cursor):
        __traceback_info__ = cursor.description
        return [ResultDescription(self._metadata_to_native_str(r[0]),
                                  # Not all drivers return lists or tuples
                                  # or things that can be sliced; psycopg2/cffi returns
                                  # an arbitrary sequence.
                                  # MySqlConnector-Python has been observed to provide
                                  # extra attributes.
                                  *list(r)[1:7])
                for r in cursor.description]

    def _rows_as_dicts(self, cursor):
        """
        An iterator of the rows as dictionaries, named by the
        lower-case column name.

        Some drivers offer the ability to do this directly when
        the statement is executed or the cursor is created;
        this is a lowest-common denominator way to do it utilizing
        DB-API 2.0 attributes.
        """
        column_descrs = self._column_descriptions(cursor)
        for row in cursor:
            result = {
                column_descr.name.lower(): column_value
                for column_descr, column_value in zip(column_descrs, row)
            }
            yield result

    def _rows_as_pretty_string(self, cursor):
        """
        Return the rows formatted in a way for easy human consumption.
        """
        # This could be tabular, but its easiest just to use pprint
        import pprint
        rows = list(self._rows_as_dicts(cursor))
        return pprint.pformat(rows)