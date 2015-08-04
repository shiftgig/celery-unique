from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import inspect
import pytz
import unittest
from uuid import uuid4

from celery.result import AsyncResult
import mock
from mockredis import mock_redis_client

import celery_unique

from tests import helpers


class CeleryUniqueTestCase(unittest.TestCase):
    def setUp(self):
        self.redis_client = mock_redis_client()


class UniqueTaskFactoryTestCase(CeleryUniqueTestCase):
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


class UniqueTaskMixinTestCase(CeleryUniqueTestCase):
    def setUp(self):
        super(UniqueTaskMixinTestCase, self).setUp()
        self.test_cls = celery_unique.unique_task_factory(helpers.SimpleFakeTaskBase)
        self.test_cls.name = 'A_TASK_NAME'
        self.test_cls.redis_client = self.redis_client


class UniqueTaskMixinMakeRedisKeyTestCase(UniqueTaskMixinTestCase):
    def test_with_static_unique_key_lambda(self):
        self.assertEqual(celery_unique.UNIQUE_REDIS_KEY_PREFIX, 'celery_unique')
        test_instance = self.test_cls()
        test_instance.unique_key = lambda *args, **kwargs: 'A_UNIQUE_KEY'
        redis_key = test_instance._make_redis_key((), {})
        self.assertEqual(redis_key, 'celery_unique:A_TASK_NAME:A_UNIQUE_KEY')

    def test_with_dynamic_unique_key_lambda(self):
        test_instance = self.test_cls()
        test_instance.unique_key = lambda *args, **kwargs: '{}.{}'.format(args[0], kwargs['four'])
        redis_key = test_instance._make_redis_key(callback_args=(1, 2), callback_kwargs={'three': 3, 'four': 4})
        self.assertEqual(redis_key, 'celery_unique:A_TASK_NAME:1.4')


class UniqueTaskMixinRevokeExtantUniqueTaskIfExistsTestCase(UniqueTaskMixinTestCase):
    def setUp(self):
        super(UniqueTaskMixinRevokeExtantUniqueTaskIfExistsTestCase, self).setUp()
        self.mock_celery_app = mock.Mock()
        self.mock_async_result = mock.Mock()
        self.mock_celery_app.AsyncResult.return_value = self.mock_async_result
        self.test_instance = self.test_cls()
        self.test_unique_task_key = 'celery_unique:A_TASK_NAME:1.4'
        self.assertIsNone(self.test_instance.redis_client.get(self.test_unique_task_key))
        self.test_instance.app = self.mock_celery_app

    def test_does_not_revoke_when_not_found_in_redis(self):
        self.test_instance._revoke_extant_unique_task_if_exists(self.test_unique_task_key)
        self.assertFalse(self.mock_async_result.called)
        self.assertFalse(self.test_instance.app.AsyncResult.called)

    def test_does_not_delete_from_redis_when_not_found_in_redis(self):
        with mock.patch.object(self.test_instance.redis_client, 'delete') as mock_redis_client_delete:
            self.test_instance._revoke_extant_unique_task_if_exists(self.test_unique_task_key)
        self.assertFalse(mock_redis_client_delete.called)

    def test_revokes_async_result_when_found_in_redis(self):
        test_task_id = str(uuid4())
        self.test_instance.redis_client.set(self.test_unique_task_key, test_task_id)
        self.assertEqual(self.test_instance.redis_client.get(self.test_unique_task_key), test_task_id)
        self.test_instance._revoke_extant_unique_task_if_exists(self.test_unique_task_key)
        self.assertTrue(self.mock_celery_app.AsyncResult.called)
        self.assertEqual(self.mock_celery_app.AsyncResult.call_count, 1)
        self.assertIsNone(self.mock_celery_app.AsyncResult.assert_called_once_with(test_task_id))
        self.assertTrue(self.mock_async_result.revoke.called)

    def test_deletes_from_redis_when_found_in_redis(self):
        test_task_id = str(uuid4())
        self.test_instance.redis_client.set(self.test_unique_task_key, test_task_id)
        self.assertIsNotNone(self.test_instance.redis_client.get(self.test_unique_task_key))
        self.assertEqual(self.test_instance.redis_client.get(self.test_unique_task_key), test_task_id)
        self.test_instance._revoke_extant_unique_task_if_exists(self.test_unique_task_key)
        self.assertNotEqual(self.test_instance.redis_client.get(self.test_unique_task_key), test_task_id)
        self.assertIsNone(self.test_instance.redis_client.get(self.test_unique_task_key))


class UniqueTaskMixinCreateUniqueTaskRecordTestCase(UniqueTaskMixinTestCase):
    def setUp(self):
        super(UniqueTaskMixinCreateUniqueTaskRecordTestCase, self).setUp()
        self.test_unique_task_key = 'celery_unique:A_TASK_NAME:1.4'
        self.test_instance = self.test_cls()
        self.assertIsNone(self.test_instance.redis_client.get(self.test_unique_task_key))

    def test_redis_record_created(self):
        test_task_id = str(uuid4())
        test_ttl_seconds = 10
        self.test_instance._create_unique_task_record(self.test_unique_task_key, test_task_id, test_ttl_seconds)
        redis_value = self.test_instance.redis_client.get(self.test_unique_task_key)
        self.assertIsNotNone(redis_value)
        self.assertEqual(redis_value, test_task_id)
        redis_record_ttl = self.test_instance.redis_client.ttl(self.test_unique_task_key)
        self.assertLessEqual(redis_record_ttl, test_ttl_seconds)

    def test_redis_set_method_called_with_expected_arguments(self):
        test_task_id = str(uuid4())
        test_ttl_seconds = 10
        with mock.patch.object(self.test_instance.redis_client, 'set') as mock_redis_client_set:
            self.test_instance._create_unique_task_record(self.test_unique_task_key, test_task_id, test_ttl_seconds)
        self.assertTrue(mock_redis_client_set.called)
        self.assertIsNone(
            mock_redis_client_set.assert_called_once_with(self.test_unique_task_key, test_task_id, ex=test_ttl_seconds)
        )


class UniqueTaskMixinMakeTTLForUniqueTaskRecordTestCase(UniqueTaskMixinTestCase):
    def test_ttl_is_difference_between_now_and_eta_if_eta_in_task_options_without_expiry(self):
        test_current_datetime_now_value = datetime.datetime.now()
        test_task_options = {'eta': test_current_datetime_now_value + datetime.timedelta(days=1)}
        expected_ttl = int((test_task_options['eta'] - test_current_datetime_now_value).total_seconds())
        self.assertGreater(expected_ttl, 0)
        with mock.patch.object(celery_unique, 'datetime', mock.Mock(wraps=datetime)) as mocked_datetime:
            mocked_datetime.datetime.now.return_value = test_current_datetime_now_value
            actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(actual_ttl, expected_ttl)

    def test_ttl_is_difference_between_now_and_eta_if_eta_in_task_options_without_expiry_can_be_timezone_aware(self):
        eastern_timezone = pytz.timezone('US/Eastern')
        test_current_datetime_now_value = datetime.datetime.now()
        test_task_options = {'eta': test_current_datetime_now_value + datetime.timedelta(days=1)}
        expected_ttl = int((test_task_options['eta'] - test_current_datetime_now_value).total_seconds())
        self.assertGreater(expected_ttl, 0)
        with mock.patch.object(celery_unique, 'datetime', mock.Mock(wraps=datetime)) as mocked_datetime:
            mocked_datetime.datetime.now.return_value = test_current_datetime_now_value
            actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(actual_ttl, expected_ttl)

    def test_ttl_defaults_to_1_if_eta_before_now_in_task_options_without_expiry(self):
        test_current_datetime_now_value = datetime.datetime.now()
        test_task_options = {'eta': test_current_datetime_now_value - datetime.timedelta(days=1)}
        expected_default_ttl = 1
        would_be_ttl = int((test_task_options['eta'] - test_current_datetime_now_value).total_seconds())
        self.assertLessEqual(would_be_ttl, 0)
        with mock.patch.object(celery_unique, 'datetime', mock.Mock(wraps=datetime)) as mocked_datetime:
            mocked_datetime.datetime.now.return_value = test_current_datetime_now_value
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
        with mock.patch.object(celery_unique, 'datetime', mock.Mock(wraps=datetime)) as mocked_datetime:
            mocked_datetime.datetime = helpers.make_frozen_datetime(test_current_datetime_now_value)
            actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(expected_ttl, actual_ttl)

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
        with mock.patch.object(celery_unique, 'datetime', mock.Mock(wraps=datetime)) as mocked_datetime:
            mocked_datetime.datetime = helpers.make_frozen_datetime(test_current_datetime_now_value)
            actual_ttl = celery_unique.UniqueTaskMixin._make_ttl_for_unique_task_record(test_task_options)
        self.assertEqual(expected_ttl, actual_ttl)


class UniqueTaskMixinApplyAsyncTestCase(UniqueTaskMixinTestCase):
    def setUp(self):
        super(UniqueTaskMixinApplyAsyncTestCase, self).setUp()
        self.test_unique_redis_key = 'celery_unique:A_TASK_NAME:1.4'
        self.unique_key_lambda = lambda a, b, c, d: '{}.{}'.format(a, d)

    def test_does_not_handle_as_unique_task_when_unique_key_is_not_callable(self):
        test_instance = self.test_cls()
        test_instance.app = mock.Mock()
        self.assertFalse(callable(test_instance.unique_key))
        self.assertIsNotNone(test_instance.redis_client)
        with mock.patch.object(self.redis_client, 'get') as mock_redis_client_get:
            rs = test_instance.apply_async(
                args=(1, 2, 3, 4),
                eta=datetime.datetime.now() + datetime.timedelta(days=1)
            )
        self.assertIsInstance(rs, AsyncResult)
        self.assertFalse(mock_redis_client_get.called)

    def test_does_not_handle_as_unique_task_when_no_eta_or_countdown_in_options(self):
        test_instance = self.test_cls()
        test_instance.app = mock.Mock()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(test_instance.redis_client)
        with mock.patch.object(self.redis_client, 'get') as mock_redis_client_get:
            rs = test_instance.apply_async(args=(1, 2, 3, 4))
        self.assertIsInstance(rs, AsyncResult)
        self.assertFalse(mock_redis_client_get.called)

    def test_does_not_handle_as_unique_task_when_redis_client_is_None(self):
        test_instance = self.test_cls()
        test_instance.app = mock.Mock()
        test_instance.redis_client = None
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNone(test_instance.redis_client)
        with mock.patch.object(self.redis_client, 'get') as mock_redis_client_get:
            rs = test_instance.apply_async(
                args=(1, 2, 3, 4),
                eta=datetime.datetime.now() + datetime.timedelta(days=1)
            )
        self.assertIsInstance(rs, AsyncResult)
        self.assertFalse(mock_redis_client_get.called)

    def test_attempts_to_revoke_extant_task_when_eta_is_given_with_no_countdown(self):
        test_instance = self.test_cls()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(test_instance.redis_client)
        with mock.patch.object(self.redis_client, 'get') as mock_redis_client_get:
            mock_redis_client_get.return_value = None
            rs = test_instance.apply_async(
                args=(1, 2, 3, 4),
                eta=datetime.datetime.now() + datetime.timedelta(days=1)
            )
        self.assertIsInstance(rs, AsyncResult)
        self.assertTrue(mock_redis_client_get.called)
        self.assertIsNone(mock_redis_client_get.assert_called_once_with(self.test_unique_redis_key))

    def test_attempts_to_revoke_extant_task_when_countdown_is_given_with_no_eta(self):
        test_instance = self.test_cls()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(test_instance.redis_client)
        with mock.patch.object(self.redis_client, 'get') as mock_redis_client_get:
            mock_redis_client_get.return_value = None
            rs = test_instance.apply_async(
                args=(1, 2, 3, 4),
                countdown=100
            )
        self.assertIsInstance(rs, AsyncResult)
        self.assertTrue(mock_redis_client_get.called)
        self.assertIsNone(mock_redis_client_get.assert_called_once_with(self.test_unique_redis_key))

    def test_revokes_extant_task_when_one_exists(self):
        test_extant_tracked_task_id = str(uuid4())
        self.assertIsNotNone(test_extant_tracked_task_id)
        self.redis_client.set(self.test_unique_redis_key, test_extant_tracked_task_id)
        self.assertEqual(self.redis_client.get(self.test_unique_redis_key), test_extant_tracked_task_id)
        test_instance = self.test_cls()
        test_instance.app = mock.Mock()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(test_instance.redis_client)
        rs = test_instance.apply_async(
            args=(1, 2, 3, 4),
            countdown=100
        )
        self.assertIsInstance(rs, AsyncResult)
        self.assertNotEqual(self.redis_client.get(self.test_unique_redis_key), test_extant_tracked_task_id)

    def test_creates_new_task_record_when_extant_task_exists(self):
        test_extant_tracked_task_id = str(uuid4())
        self.assertIsNotNone(test_extant_tracked_task_id)
        self.redis_client.set(self.test_unique_redis_key, test_extant_tracked_task_id)
        self.assertEqual(self.redis_client.get(self.test_unique_redis_key), test_extant_tracked_task_id)
        test_instance = self.test_cls()
        test_instance.app = mock.Mock()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(test_instance.redis_client)
        rs = test_instance.apply_async(
            args=(1, 2, 3, 4),
            countdown=100
        )
        self.assertIsInstance(rs, AsyncResult)
        self.assertNotEqual(self.redis_client.get(self.test_unique_redis_key), test_extant_tracked_task_id)
        self.assertEqual(self.redis_client.get(self.test_unique_redis_key), rs.task_id)

    def test_creates_new_task_record_when_no_extant_task_exists(self):
        self.assertIsNone(self.redis_client.get(self.test_unique_redis_key))
        test_instance = self.test_cls()
        test_instance.app = mock.Mock()
        test_instance.unique_key = self.unique_key_lambda
        self.assertTrue(callable(test_instance.unique_key))
        self.assertIsNotNone(test_instance.redis_client)
        rs = test_instance.apply_async(
            args=(1, 2, 3, 4),
            countdown=100
        )
        self.assertIsInstance(rs, AsyncResult)
        self.assertEqual(self.redis_client.get(self.test_unique_redis_key), rs.task_id)
