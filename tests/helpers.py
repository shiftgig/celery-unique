from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from uuid import uuid4

from celery.result import AsyncResult


class SimpleFakeTaskBase(object):
    def apply_async(self, *args, **kwargs):
        return AsyncResult(str(uuid4()))
