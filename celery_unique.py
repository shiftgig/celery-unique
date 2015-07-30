# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import datetime


UNIQUE_REDIS_KEY_PREFIX = 'celery_unique'


def unique_task_factory(task_cls, redis_client):
    class UniqueTask(task_cls):
        abstract = True
        unique_key = None

        def apply_async(self,
                        args=None,
                        kwargs=None,
                        task_id=None,
                        producer=None,
                        link=None,
                        link_error=None,
                        **options):
            should_handle_as_unique_task = (
                callable(self.unique_key) and ('eta' in options.keys() or 'countdown' in options.keys())
            )

            if should_handle_as_unique_task:
                unique_redis_key = self._make_redis_key(args, kwargs)
                self._remove_extant_unique_task_record(unique_redis_key)

            rv = super(UniqueTask, self).apply_async(args, kwargs, task_id, producer, link, link_error, **options)

            if should_handle_as_unique_task:
                ttl = self._make_ttl_for_unique_task_record(options)
                self._create_unique_task_record(unique_redis_key, rv.task_id, ttl)

            return rv

        def _make_redis_key(self, callback_args, callback_kwargs):
            return '{prefix}:{task_name}:{unique_key}'.format(
                prefix=UNIQUE_REDIS_KEY_PREFIX,
                task_name=self.name,
                unique_key=self.unique_key(*callback_args, **callback_kwargs)
            )

        @staticmethod
        def _remove_extant_unique_task_record(redis_key):
            redis_client.delete(redis_key)

        @staticmethod
        def _create_unique_task_record(redis_key, task_id, ttl):
            redis_client.set(redis_key, task_id, ex=ttl)

        @staticmethod
        def _make_ttl_for_unique_task_record(task_options):
            # Set a default TTL as 1 second (in case actual TTL already occurred)
            ttl_seconds = 1

            option_keys = task_options.keys()
            if 'etl' in option_keys:
                ttl_seconds = int((task_options['etl'] - datetime.datetime.now()).total_seconds())
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

    return UniqueTask
