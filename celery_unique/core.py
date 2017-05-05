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
        """Apply tasks asynchronously by sending a message.

        This method serves either as a wrapper for `celery.Task.apply_async()` or, if the task decorator
        was configured with a `redis_client`, callable `unique_key` and `apply_async()` was called with
        either an `eta` or `countdown` argument, the task will be treated as unique.  In these cases,
        this method will first revoke any extant task which matches the same unique key configuration
        before proceeding to publish the task.  Before returning, a unique task's identifying unique key
        will be saved to Redis as a key, with its task id (provided by the newly-created `AsyncResult` instance)
        serving as the value.

        @see `celery.Task.apply_async()`
        """
        should_handle_as_unique_task = (
            callable(self.unique_key)
            and ('eta' in options.keys() or 'countdown' in options.keys())
            and self.redis_client is not None
        )

        if should_handle_as_unique_task:
            # Generate the unique redis key and revoke any task that shares the same key (if one exists)
            unique_redis_key = self._make_redis_key(args, kwargs)
            self._revoke_extant_unique_task_if_exists(unique_redis_key)

        # Pass the task along to Celery for publishing and intercept the AsyncResult return value
        rv = super(UniqueTaskMixin, self).apply_async(args, kwargs, task_id, producer, link, link_error, **options)

        if should_handle_as_unique_task:
            # Create a Redis key/value pair to serve as a tracking record for the newly-created task.
            # The new record will be given a TTL that allows it to expire (approximately) at the same time
            # that the task is executed.
            ttl = self._make_ttl_for_unique_task_record(options)
            self._create_unique_task_record(unique_redis_key, rv.task_id, ttl)

        return rv

    def _make_redis_key(self, callback_args, callback_kwargs):
        """Creates a key used to identify the task's unique configuration in Redis.

        @note All positional arguments and/or keyword arguments sent to the task are applied identically to
        the task's bound `unique_key` callable.

        @param callback_args: The positional arguments which will be passed to the task when it executes
        @type callback_args: list | tuple
        @param callback_kwargs: The keyword arguments which will be passed to the task when it executes
        @type callback_kwargs: dict

        @return: The key which will be used to find any extant version of this task which, if found,
            will by revoked.  Keys are built by using three colon-delimited components:
                1. A global prefix used to identify that the key/value pair in Redis was created to track
                a unique Celery task (by default, this is "celery_unique")
                2. The name of the task (usually the Python dot-notation path to the function)
                3. The value produced by the `key_generator` callable when supplied with the task's callback
                arguments.
        @rtype: unicode
        """
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
        """Given a Redis key, deletes the corresponding record if one exists.

        @param redis_key: The string (potentially) used by Redis as the key for the record
        @type redis_key: str | unicode
        """
        task_id = self.redis_client.get(redis_key)
        if task_id is not None:
            self.app.AsyncResult(task_id).revoke()
            self.redis_client.delete(redis_key)

    def _create_unique_task_record(self, redis_key, task_id, ttl):
        """Creates a new Redis key/value pair for the recently-published unique task.

        @param redis_key: The unique key which identifies the task and its configuration (expected to be produced
            by the `UniqueTaskMixin._make_redis_key()` method).
        @type redis_key: str | unicode
        @param task_id: The ID of the recently-published unique task, which will be used as the Redis value
        @param ttl: The TTL for the Redis record, which should be (approximately) equal to the number of seconds
            remaining until the earliest time that the task is expected to be executed by Celery.
        """
        self.redis_client.set(redis_key, task_id, ex=ttl)

    @staticmethod
    def _make_ttl_for_unique_task_record(task_options):
        """Given the options provided to `apply_async()` as keyword arguments, determines the appropriate
        TTL to ensure that a unique task record in Redis expires (approximately) at the same time as the earliest
        time that the task is expected to be executed by Celery.

        The TTL value will be determined by examining the following values, in order of preference:
            - The `eta` keyword argument passed to `apply_async()`, if any.  If this value is found,
            then the TTL will be the number of seconds between now and the ETA datetime.
            - The `countdown` keyword argument passed to `apply_async()`, which will theoretically always
            exist if `eta` was not provided.  If this value is used, the TTL will be equal.

        Additionally, if an `expires` keyword argument was passed, and its value represents (either as an integer
        or timedelta) a shorter duration of time than the values provided by `eta` or `countdown`, the TTL will be
        reduced to the value of `countdown`.

        Finally, the TTL value returned by this method will always be greater than or equal to 1, in order to ensure
        compatibility with Redis' TTL requirements, and that a record produced for a nonexistent task will only
        live for a maximum of 1 second.

        @param task_options: The values passed as additional keyword arguments to `apply_async()`
        @type task_options: dict

        @return: The TTL (in seconds) for the Redis record to-be-created
        @rtype: int
        """
        # Set a default TTL as 1 second (in case actual TTL already occurred)
        ttl_seconds = 1

        option_keys = task_options.keys()
        if 'eta' in option_keys:
            # Get the difference between the ETA and now (relative to the ETA's timezone)
            ttl_seconds = int(
                (task_options['eta'] - datetime.datetime.now(tz=task_options['eta'].tzinfo)).total_seconds()
            )
        elif 'countdown' in option_keys:
            ttl_seconds = task_options['countdown']

        if 'expires' in option_keys:
            if isinstance(task_options['expires'], datetime.datetime):
                # Get the difference between the countdown and now (relative to the countdown's timezone)
                seconds_until_expiry = int(
                    (task_options['expires'] - datetime.datetime.now(tz=task_options['expires'].tzinfo)).total_seconds()
                )
            else:
                seconds_until_expiry = task_options['expires']
            if seconds_until_expiry < ttl_seconds:
                ttl_seconds = seconds_until_expiry

        if ttl_seconds <= 0:
            ttl_seconds = 1

        return ttl_seconds


def unique_task_factory(task_cls):
    """Creates a new, abstract Celery Task class that enables properly-configured Celery tasks to uniquely exist.

    @param task_cls: The original base class which should used with UniqueTaskMixin to produce a new Celery task
        base class.
    @type task_cls: type

    @return: The new Celery task base class with unique task-handling functionality mixed in.
    @rtype: type
    """
    return type(str('UniqueTask'), (UniqueTaskMixin, task_cls), {})
