from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import inspect
import unittest

from mockredis import mock_redis_client

import celery_unique


class FakeTaskBase(object):
    def __init__(self):
        pass


class CeleryUniqueTestCase(unittest.TestCase):
    def setUp(self):
        self.redis_client = mock_redis_client()


class UniqueTaskFactoryTestCase(CeleryUniqueTestCase):
    def test_return_value_is_class(self):
        test_cls = celery_unique.unique_task_factory(FakeTaskBase, self.redis_client)
        self.assertTrue(inspect.isclass(test_cls))

    def test_return_value_type_is_type(self):
        test_cls = celery_unique.unique_task_factory(FakeTaskBase, self.redis_client)
        self.assertIs(type(test_cls), type)

    def test_return_value_is_subclass_of_given_task_class(self):
        given_task_class = FakeTaskBase
        test_cls = celery_unique.unique_task_factory(given_task_class, self.redis_client)
        self.assertTrue(issubclass(test_cls, given_task_class))

    def test_return_value_class_method_resolution_order(self):
        given_task_class = FakeTaskBase
        test_cls = celery_unique.unique_task_factory(given_task_class, self.redis_client)
        self.assertListEqual(test_cls.mro(), [test_cls, given_task_class, object])


class UniqueTaskMakeRedisKeyTestCase(CeleryUniqueTestCase):
    def setUp(self):
        super(UniqueTaskMakeRedisKeyTestCase, self).setUp()
        self.test_cls = celery_unique.unique_task_factory(FakeTaskBase, self.redis_client)
        self.test_cls.name = 'A_TASK_NAME'

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
