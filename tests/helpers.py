from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import datetime
from uuid import uuid4

from celery.result import AsyncResult


class SimpleFakeTaskBase(object):
    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, **options):
        return AsyncResult(str(uuid4()))


def make_frozen_datetime(freeze_point):
    """Produces a subclass of `datetime.datetime` where the `now()` method always returns the same, known value.

    @note This is a bit of a monstrosity, but it serves a useful purpose here when we mock the `datetime` module
    for a specific target which, in addition to calling `datetime.datetime.now()`, will also be calling
    `isinstance(something, datetime.datetime)`.  The usual mocked approach causes a failure since, within the
    patched context, `datetime.datetime` is an *instance* of `mock.Mock`, whereas Python requires that the
    second argument to `isinstance()` must be a class, type, or tuple of classes and types.

    @param freeze_point: The value that the should always be returned when the `now()` method is called on the
        class returned by this factory function.  This could be anything, although it should probably be
        an instance of `datetime.datetime`.

    @return: A new subclass of `datetime.datetime` whose `now()` method will always return the value provided by
        the `freeze_point` argument (or specifically, the value of its `_now_constant` class variable.
    @rtype: type
    """
    MetaDatetime = type(
        b'MetaDatetime',
        (type,),
        {
            '__instancecheck__': (lambda c, o: isinstance(o, datetime.datetime)),
            '_now_constant': freeze_point
        }
    )
    OverrideDatetime = type(b'datetime', (datetime.datetime,), {'now': classmethod(lambda cls, tz: cls._now_constant)})
    return MetaDatetime(b'datetime', (OverrideDatetime,), {})
