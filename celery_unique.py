# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import datetime


UNIQUE_REDIS_KEY_PREFIX = 'celery_unique'


class UniqueTaskMixin(object):
    abstract = True
    unique_key = None
    redis_client = None

    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None, link=None, link_error=None, **options):
        should_handle_as_unique_task = (
            callable(self.unique_key)
            and ('eta' in options.keys() or 'countdown' in options.keys())
            and self.redis_client is not None
        )

        if should_handle_as_unique_task:
            unique_redis_key = self._make_redis_key(args, kwargs)
            self._revoke_extant_unique_task_if_exists(unique_redis_key)

        rv = super(UniqueTaskMixin, self).apply_async(args, kwargs, task_id, producer, link, link_error, **options)

        if should_handle_as_unique_task:
            ttl = self._make_ttl_for_unique_task_record(options)
            self._create_unique_task_record(unique_redis_key, rv.task_id, ttl)

        return rv

    def _make_redis_key(self, callback_args, callback_kwargs):
        # Get the unbound lambda used to create `self.unique_key` if the inner function exists
        key_generator = self.unique_key.__func__ if hasattr(self.unique_key, '__func__') else self.unique_key

        # Create and return the redis key with the generated unique key suffix
        return '{prefix}:{task_name}:{unique_key}'.format(
            prefix=UNIQUE_REDIS_KEY_PREFIX,
            task_name=self.name,
            unique_key=key_generator(
                *(callback_args or ()),
                **(callback_kwargs or {})
            )
        )

    def _revoke_extant_unique_task_if_exists(self, redis_key):
        task_id = self.redis_client.get(redis_key)
        if task_id is not None:
            self.app.AsyncResult(task_id).revoke()
            self.redis_client.delete(redis_key)

    def _create_unique_task_record(self, redis_key, task_id, ttl):
        self.redis_client.set(redis_key, task_id, ex=ttl)

    @staticmethod
    def _make_ttl_for_unique_task_record(task_options):
        # Set a default TTL as 1 second (in case actual TTL already occurred)
        ttl_seconds = 1

        option_keys = task_options.keys()
        if 'eta' in option_keys:
            ttl_seconds = int((task_options['eta'] - datetime.datetime.now()).total_seconds())
        elif 'countdown' in option_keys:
            ttl_seconds = task_options['countdown']

        if 'expires' in option_keys:
            if isinstance(task_options['expires'], datetime.datetime):
                seconds_until_expiry = int((task_options['expires'] - datetime.datetime.now()).total_seconds())
            else:
                seconds_until_expiry = task_options['expires']
            if seconds_until_expiry < ttl_seconds:
                ttl_seconds = seconds_until_expiry

        if ttl_seconds <= 0:
            ttl_seconds = 1

        return ttl_seconds


def unique_task_factory(task_cls):
    return type(b'UniqueTask', (UniqueTaskMixin, task_cls), {})
