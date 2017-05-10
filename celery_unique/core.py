# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import datetime

UNIQUE_KEY_PREFIX = 'celery_unique'


class UniqueTaskMixin(object):
    abstract = True
    unique_key = None
    unique_backend = None

    def apply_async(self, args=(), kwargs={}, task_id=None, **options):
        """
        Apply tasks asynchronously by sending a message.

        This method serves either as a wrapper for `celery.Task.apply_async()`
        or, if the task decorator was configured with a `unique_backend`,
        callable `unique_key` and `apply_async()` was called with either an
        `eta` or `countdown` argument, the task will be treated as unique.

        In these cases, this method will first revoke any extant task which
        matches the same unique key configuration before proceeding to publish
        the task.  Before returning, a unique task's identifying unique key
        will be saved to Redis as a key, with its task id (provided by the
        newly-created `AsyncResult` instance) serving as the value.

        See ``celery.Task.apply_async()``

        :param func unique_key: Function used to generate a unique key to
            identify this task. The function will take receive the same args
            and kwargs the task is passed.
        :param UniquenessBackend backend: A backend to use to cache queued
            tasks and to determine is a task is unique or not.
        """
        should_handle_as_unique_task = self._handle_as_unique(options)

        if should_handle_as_unique_task:
            # Generate the unique key and revoke any task that shares the same
            # key (if one exists)
            key = self.make_key(args, kwargs)
            self._revoke_extant_task(key)

        # Pass the task along to Celery for publishing and intercept the
        # AsyncResult return value
        rv = super(UniqueTaskMixin, self).apply_async(args, kwargs, task_id, **options)

        if should_handle_as_unique_task:
            # Inform the backend of this tasks.
            # The new record will be given a TTL that allows it to expire
            # (approximately) at the same time that the task is executed.
            ttl = self._make_ttl_for_unique_task_record(options)
            self.unique_backend.create_task_record(key, rv.task_id, ttl)

        return rv

    def _handle_as_unique(self, options):
        """
        Determines if a task should be handles as unique.

        :param dict options: The options dict passed to `apply_async`.
        """
        return (
            callable(self.unique_key)
            and ('eta' in options or 'countdown' in options)
            and self.unique_backend is not None
        )

    def _revoke_extant_task(self, key):
        """
        Removes an extant task

        Not that this removed the key from the store regardless of whether the
        task is still queued or not.
        """
        task_id = self.unique_backend.get_task_id(key)
        if task_id is not None:
            self.app.AsyncResult(task_id).revoke()
            self.unique_backend.revoke_extant_task(key)

    def make_key(self, callback_args, callback_kwargs):
        """
        Creates a key used to identify the task's unique configuration in Redis.

        Note: All positional arguments and/or keyword arguments sent to the
        task are applied identically to the task's bound `unique_key` callable.

        :param callback_args: The positional arguments which will be passed to
            the task when it executes @type callback_args: list | tuple
        :param callback_kwargs: The keyword arguments which will be passed to
            the task when it executes @type callback_kwargs: dict

        :return: The key which will be used to find any extant version of this
        task which, if found, will by revoked.  Keys are built by using three
        colon-delimited components::

            1. A global prefix used to identify that the key/value pair in
                the backend was created to track
                a unique Celery task (by default, this is "celery_unique")
            2. The name of the task (usually the Python dot-notation path to
                the function)
            3. The value produced by the `key_generator` callable when supplied
                with the task's callback arguments.

        :rtype: unicode
        """

        # Get the unbound lambda used to create `self.unique_key` if the inner
        # function exists
        if hasattr(self.unique_key, '__func__'):
            key_generator = self.unique_key.__func__
        else:
            key_generator = self.unique_key

        # Create and return the redis key with the generated unique key suffix
        return '{prefix}:{task_name}:{unique_key}'.format(
            prefix=UNIQUE_KEY_PREFIX,
            task_name=self.name,
            unique_key=key_generator(*callback_args, **callback_kwargs),
        )

    @staticmethod
    def _make_ttl_for_unique_task_record(task_options):
        """
        Calculate an aproximate TTL for an enqueued task.

        Given the options provided to `apply_async()` as keyword arguments,
        determines the appropriate TTL to ensure that a unique task record
        expires (approximately) at the same time as the earliest time that the
        task is expected to be executed by Celery.

        The TTL value will be determined by examining the following values, in
        order of preference:
            - The `eta` keyword argument passed to `apply_async()`, if any.  If
              this value is found, then the TTL will be the number of seconds
              between now and the ETA datetime.
            - The `countdown` keyword argument passed to `apply_async()`, which
              will theoretically always exist if `eta` was not provided.  If
              this value is used, the TTL will be equal.

        Additionally, if an `expires` keyword argument was passed, and its
        value represents (either as an integer or timedelta) a shorter duration
        of time than the values provided by `eta` or `countdown`, the TTL will
        be reduced to the value of `countdown`.

        Finally, the TTL value returned by this method will always be greater
        than or equal to 1, in order to ensure compatibility with different
        backend's TTL requirements, and that a record produced for a
        nonexistent task will only live for a maximum of 1 second.

        :param dict task_options: The values passed as additional keyword
            arguments to `apply_async()`

        :return: The TTL (in seconds) for the Redis record to-be-created
        :rtype: int
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
