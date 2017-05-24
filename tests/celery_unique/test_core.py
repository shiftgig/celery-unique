from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import inspect
from unittest import TestCase

try:
    from unittest import mock
except ImportError:
    import mock

from celery import uuid
from celery.result import AsyncResult
from freezegun import freeze_time
from mockredis import mock_redis_client

import celery_unique
from celery_unique.backends import RedisBackend
from tests import helpers


class UniqueTaskFactoryTestCase(TestCase):

    def test_return_value_is_class(self):
        test_cls = celery_unique.unique_task_factory(helpers.SimpleFakeTaskBase)
        self.assertTrue(inspect.isclass(test_cls))

    def test_return_value_type_is_type(self):
        test_cls = celery_unique.unique_task_factory(helpers.SimpleFakeTaskBase)
        self.assertIs(type(test_cls), type)

    def test_return_value_is_subclass_of_given_task_class(self):
        given_task_class = helpers.SimpleFakeTaskBase
        test_cls = celery_unique.unique_task_factory(given_task_class)
        self.assertTrue(issubclass(test_cls, given_task_class))

    def test_return_value_class_method_resolution_order(self):
        given_task_class = helpers.SimpleFakeTaskBase
        test_cls = celery_unique.unique_task_factory(given_task_class)
        self.assertListEqual(test_cls.mro(), [test_cls, celery_unique.UniqueTaskMixin, given_task_class, object])


class UniqueTaskMixinTestCase(TestCase):
    """Helper base class used to test UniqueTaskMixin methods."""

    def setUp(self):
        super(UniqueTaskMixinTestCase, self).setUp()
        self.redis_client = mock_redis_client()
        self.test_cls = celery_unique.unique_task_factory(helpers.SimpleFakeTaskBase)
        self.test_cls.name = 'A_TASK_NAME'
        self.redis_client = self.redis_client
        self.test_cls.unique_backend = RedisBackend(self.redis_client)


class UniqueTaskMixinMakeKeyTestCase(UniqueTaskMixinTestCase):

    def test_with_static_unique_key_lambda(self):
        task = self.test_cls()
        task.unique_key = lambda *args, **kwargs: 'A_UNIQUE_KEY'

        self.assertEqual(
            task.make_key((), {}),
            'celery_unique:A_TASK_NAME:A_UNIQUE_KEY',
        )

    def test_with_dynamic_unique_key_lambda(self):
        task = self.test_cls()
        task.unique_key = lambda *args, **kwargs: '{}.{}'.format(
            args[0],
            kwargs['four'],
        )

        self.assertEqual(
            task.make_key(
                callback_args=(1, 2),
                callback_kwargs={'three': 3, 'four': 4}
            ),
            'celery_unique:A_TASK_NAME:1.4',
        )


class UniqueTaskMixinRevokeExtantTaskTestCase(UniqueTaskMixinTestCase):
    def setUp(self):
        super(UniqueTaskMixinRevokeExtantTaskTestCase, self).setUp()
        self.mock_celery_app = mock.Mock()
        self.mock_async_result = mock.Mock()
        self.mock_celery_app.AsyncResult.return_value = self.mock_async_result
        self.test_instance = self.test_cls()
        self.test_unique_task_key = 'celery_unique:A_TASK_NAME:1.4'
        self.assertIsNone(self.redis_client.get(self.test_unique_task_key))
        self.test_instance.app = self.mock_celery_app

    def test_does_not_revoke_when_not_found_in_redis(self):
        # TODO: continue refactor
        self.test_instance._revoke_extant_task(self.test_unique_task_key)
        self.assertFalse(self.mock_async_result.called)
        self.assertFalse(self.test_instance.app.AsyncResult.called)

    def test_does_not_delete_from_redis_when_not_found_in_redis(self):
        with mock.patch.object(self.redis_client, 'delete') as mock_delete:
            self.test_instance._revoke_extant_task(self.test_unique_task_key)
        self.assertFalse(mock_delete.called)

    def test_revokes_async_result_when_found_in_redis(self):
        test_task_id = uuid().encode()
        self.redis_client.set(self.test_unique_task_key, test_task_id)
        self.assertEqual(self.redis_client.get(self.test_unique_task_key), test_task_id)
        self.test_instance._revoke_extant_task(self.test_unique_task_key)
        self.assertEqual(self.mock_celery_app.AsyncResult.call_count, 1)
        self.assertEqual(
            self.mock_celery_app.AsyncResult.call_args,
            mock.call(test_task_id.decode()),
        )
        self.assertIsNone(self.mock_async_result.revoke.assert_called_once_with())

    def test_deletes_from_redis_when_found_in_redis(self):
        test_task_id = uuid().encode()
        self.redis_client.set(self.test_unique_task_key, test_task_id)
        self.assertIsNotNone(self.redis_client.get(self.test_unique_task_key))
        self.assertEqual(self.redis_client.get(self.test_unique_task_key), test_task_id)
        self.test_instance._revoke_extant_task(self.test_unique_task_key)
        self.assertNotEqual(self.redis_client.get(self.test_unique_task_key), test_task_id)
        self.assertIsNone(self.redis_client.get(self.test_unique_task_key))


class UniqueTaskMixinMakeTTLForUniqueTaskRecordTestCase(UniqueTaskMixinTestCase):

    @freeze_time('2017-05-05')
    def test_ttl_is_difference_between_now_and_eta_if_eta_in_task_options_without_expiry(self):
        test_current_datetime_now_value = datetime.datetime.now()
        test_task_options = {'eta': test_current_datetime_now_value + datetime.timedelta(days=1)}
        expected_ttl = int((test_task_options['eta'] - test_current_datetime_now_value).total_seconds())
        self.assertGreater(expected_ttl, 0)
        actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(actual_ttl, expected_ttl)

    @freeze_time('2017-05-05')
    def test_ttl_is_difference_between_now_and_eta_if_eta_in_task_options_without_expiry_can_be_timezone_aware(self):
        test_current_datetime_now_value = datetime.datetime.now()
        test_task_options = {'eta': test_current_datetime_now_value + datetime.timedelta(days=1)}
        expected_ttl = int((test_task_options['eta'] - test_current_datetime_now_value).total_seconds())
        self.assertGreater(expected_ttl, 0)
        actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(actual_ttl, expected_ttl)

    @freeze_time('2017-05-05')
    def test_ttl_defaults_to_1_if_eta_before_now_in_task_options_without_expiry(self):
        test_current_datetime_now_value = datetime.datetime.now()
        test_task_options = {'eta': test_current_datetime_now_value - datetime.timedelta(days=1)}
        expected_default_ttl = 1
        would_be_ttl = int((test_task_options['eta'] - test_current_datetime_now_value).total_seconds())
        self.assertLessEqual(would_be_ttl, 0)
        actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(actual_ttl, expected_default_ttl)

    def test_ttl_is_countdown_if_countdown_in_task_options_without_expiry(self):
        test_task_options = {'countdown': 100}
        expected_ttl = test_task_options['countdown']
        actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(expected_ttl, actual_ttl)

    def test_ttl_defaults_to_1_if_no_eta_or_countdown_in_task_options_without_expiry(self):
        test_task_options = {}
        expected_ttl = 1
        actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(expected_ttl, actual_ttl)

    def test_int_expires_in_task_options_overrides_ttl_if_expiry_time_is_less_than_ttl(self):
        test_task_options = {'countdown': 100, 'expires': 50}
        self.assertLess(test_task_options['expires'], test_task_options['countdown'])
        expected_ttl = test_task_options['expires']
        actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(expected_ttl, actual_ttl)

    def test_int_expires_in_task_options_does_not_override_ttl_if_expiry_time_is_greater_than_ttl(self):
        test_task_options = {'countdown': 50, 'expires': 100}
        self.assertGreater(test_task_options['expires'], test_task_options['countdown'])
        expected_ttl = test_task_options['countdown']
        actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(expected_ttl, actual_ttl)

    @freeze_time('2017-04-28')
    def test_datetime_expires_in_task_options_overrides_ttl_if_expiry_time_is_less_than_ttl(self):
        test_current_datetime_now_value = datetime.datetime.now()
        test_seconds_to_expiry = 50
        test_task_options = {
            'countdown': 100,
            'expires': test_current_datetime_now_value + datetime.timedelta(seconds=test_seconds_to_expiry),
        }
        self.assertLess(
            int((test_task_options['expires'] - test_current_datetime_now_value).total_seconds()),
            test_task_options['countdown']
        )
        expected_ttl = test_seconds_to_expiry
        actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(expected_ttl, actual_ttl)

    @freeze_time('2017-04-28')
    def test_datetime_expires_in_task_options_does_not_override_ttl_if_expiry_time_is_greater_than_ttl(self):
        test_current_datetime_now_value = datetime.datetime.now()
        test_seconds_to_expiry = 100
        test_task_options = {
            'countdown': 50,
            'expires': test_current_datetime_now_value + datetime.timedelta(seconds=test_seconds_to_expiry),
        }
        self.assertGreater(
            int((test_task_options['expires'] - test_current_datetime_now_value).total_seconds()),
            test_task_options['countdown']
        )
        expected_ttl = test_task_options['countdown']
        actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(expected_ttl, actual_ttl)


class UniqueTaskMixinApplyAsyncTestCase(UniqueTaskMixinTestCase):
    def setUp(self):
        super(UniqueTaskMixinApplyAsyncTestCase, self).setUp()
        self.test_unique_redis_key = 'celery_unique:A_TASK_NAME:1.4'
        self.unique_key_lambda = lambda a, b, c, d: '{}.{}'.format(a, d)

    def test_does_not_handle_as_unique_task_when_not_applicable(self):
        task = self.test_cls()
        task._handle_as_unique = lambda options: False

        with mock.patch.object(task, 'unique_backend') as mock_backend:
            rs = task.apply_async(
                args=(1, 2, 3, 4),
                eta=datetime.datetime.now() + datetime.timedelta(days=1)
            )

        self.assertIsInstance(rs, AsyncResult)

        # No key made or record created in the backend:
        self.assertFalse(mock_backend.make_key.called)
        self.assertFalse(mock_backend.crete_task_record.called)

    def test_attempts_to_revoke_extant_task_when_eta_is_given_with_no_countdown(self):
        test_instance = self.test_cls()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(self.redis_client)
        with mock.patch.object(self.redis_client, 'get') as mock_redis_client_get:
            mock_redis_client_get.return_value = None
            rs = test_instance.apply_async(
                args=(1, 2, 3, 4),
                eta=datetime.datetime.now() + datetime.timedelta(days=1)
            )
        self.assertIsInstance(rs, AsyncResult)
        self.assertIsNone(mock_redis_client_get.assert_called_once_with(self.test_unique_redis_key))

    def test_attempts_to_revoke_extant_task_when_countdown_is_given_with_no_eta(self):
        test_instance = self.test_cls()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(self.redis_client)
        with mock.patch.object(self.redis_client, 'get') as mock_redis_client_get:
            mock_redis_client_get.return_value = None
            rs = test_instance.apply_async(
                args=(1, 2, 3, 4),
                countdown=100
            )
        self.assertIsInstance(rs, AsyncResult)
        self.assertIsNone(mock_redis_client_get.assert_called_once_with(self.test_unique_redis_key))

    def test_revokes_extant_task_when_one_exists(self):
        test_extant_tracked_task_id = uuid().encode()
        self.assertIsNotNone(test_extant_tracked_task_id)
        self.redis_client.set(self.test_unique_redis_key, test_extant_tracked_task_id)
        self.assertEqual(self.redis_client.get(self.test_unique_redis_key), test_extant_tracked_task_id)
        test_instance = self.test_cls()
        test_instance.app = mock.Mock()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))

        self.assertIsNotNone(test_instance.unique_backend)
        rs = test_instance.apply_async(
            args=(1, 2, 3, 4),
            countdown=100
        )
        self.assertIsInstance(rs, AsyncResult)
        self.assertNotEqual(self.redis_client.get(self.test_unique_redis_key), test_extant_tracked_task_id)

    def test_creates_new_task_record_when_extant_task_exists(self):
        test_extant_tracked_task_id = uuid().encode()
        self.assertIsNotNone(test_extant_tracked_task_id)
        self.redis_client.set(self.test_unique_redis_key, test_extant_tracked_task_id)
        self.assertEqual(self.redis_client.get(self.test_unique_redis_key), test_extant_tracked_task_id)
        test_instance = self.test_cls()
        test_instance.app = mock.Mock()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(self.redis_client)
        rs = test_instance.apply_async(
            args=(1, 2, 3, 4),
            countdown=100
        )
        self.assertIsInstance(rs, AsyncResult)
        self.assertNotEqual(self.redis_client.get(self.test_unique_redis_key), test_extant_tracked_task_id)
        self.assertEqual(self.redis_client.get(self.test_unique_redis_key), rs.task_id.encode())

    def test_creates_new_task_record_when_no_extant_task_exists(self):
        self.assertIsNone(self.redis_client.get(self.test_unique_redis_key))

        test_instance = self.test_cls()
        test_instance.app = mock.Mock()
        test_instance.unique_key = self.unique_key_lambda

        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(test_instance.unique_backend)
        rs = test_instance.apply_async(
            args=(1, 2, 3, 4),
            countdown=100
        )
        self.assertIsInstance(rs, AsyncResult)
        self.assertEqual(
            self.redis_client.get(self.test_unique_redis_key),
            rs.task_id.encode(),
        )


class UniqueTaskMixinHandleAsUniqueTestCase(UniqueTaskMixinTestCase):

    def test_is_unique(self):
        task = self.test_cls()
        self.assertIsNotNone(task.unique_backend)
        task.unique_key = lambda *a, **kw: 'the_key'
        self.assertEqual(task._handle_as_unique({'eta': 10}), True)

    def test_no_key_func(self):
        task = self.test_cls()
        self.assertIsNotNone(task.unique_backend)
        self.assertIsNone(task.unique_key)
        self.assertEqual(task._handle_as_unique({'eta': 10}), False)

    def test_no_eta_or_countdown(self):
        task = self.test_cls()
        task.unique_key = lambda *a, **kw: 'the_key'
        self.assertIsNotNone(task.unique_backend)
        self.assertEqual(task._handle_as_unique({}), False)

    def test_no_backend(self):
        task = self.test_cls()
        task.unique_backend = None
        task.unique_key = lambda *a, **kw: 'the_key'
        self.assertEqual(task._handle_as_unique({'eta': 10}), False)
