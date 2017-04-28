from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from uuid import uuid4

from celery.result import AsyncResult


class SimpleFakeTaskBase(object):
    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, **options):
        return AsyncResult(str(uuid4()))
