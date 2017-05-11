from unittest import TestCase

from mockredis import mock_redis_client

from celery_unique.backends import InMemoryBackend, RedisBackend


class RedisBackendTestCase(TestCase):
    """Base class used to test RedisBackend methods."""

    def setUp(self):
        self.redis_client = mock_redis_client()
        self.backend = RedisBackend(self.redis_client)


class RedisBackendCreateTaskRecordTestCase(RedisBackendTestCase):

    def test_record_created(self):
        task_id = '6bf813e5-74aa-4f12-a308-7e0a4bec5916'
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'
        task_ttl = 10

        self.backend.create_task_record(task_key, task_id, task_ttl)

        self.assertEqual(self.redis_client.get(task_key).decode(), task_id)
        self.assertEqual(self.backend.get_task_id(task_key), task_id)

        self.assertLessEqual(self.redis_client.ttl(task_key), task_ttl)


class RedisBackendRevokeExtantRecordTestCase(RedisBackendTestCase):

    def test_revoke_existing(self):
        task_id = '6bf813e5-74aa-4f12-a308-7e0a4bec5916'
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'

        self.redis_client.set(task_key, task_id)
        self.assertEqual(self.redis_client.keys(), [task_key.encode()])

        self.backend.revoke_extant_task(task_key)
        self.assertEqual(self.redis_client.keys(), [])

    def test_revoke_inexistant(self):
        # Note: this test also validates that we handle revoking inexistant
        # tasks without any exception:
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'

        self.assertEqual(self.redis_client.keys(), [])
        self.backend.revoke_extant_task(task_key)
        self.assertEqual(self.redis_client.keys(), [])


class RedisBackendGetTaskIdTestCase(RedisBackendTestCase):

    def test_get_existing(self):
        task_id = '6bf813e5-74aa-4f12-a308-7e0a4bec5916'
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'

        self.redis_client.set(task_key, task_id)
        self.assertEqual(self.backend.get_task_id(task_key), task_id)

    def test_get_inexisting(self):
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'

        self.assertEqual(self.redis_client.keys(), [])
        self.assertIsNone(self.backend.get_task_id(task_key))


class InMemoryBackendTestCase(TestCase):
    """Base class used to test InMemoryBackend methods."""

    def setUp(self):
        self.backend = InMemoryBackend()


class InMemoryBackendCreateTaskRecordTestCase(InMemoryBackendTestCase):

    def test_record_created(self):
        task_id = '6bf813e5-74aa-4f12-a308-7e0a4bec5916'
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'

        self.backend.create_task_record(task_key, task_id)

        self.assertEqual(self.backend.tasks.get(task_key), task_id)
        self.assertEqual(self.backend.get_task_id(task_key), task_id)


class InMemoryBackendRevokeExtantRecordTestCase(InMemoryBackendTestCase):

    def test_revoke_existing(self):
        task_id = '6bf813e5-74aa-4f12-a308-7e0a4bec5916'
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'

        self.backend.tasks[task_key] = task_id
        self.assertEqual(self.backend.tasks, {task_key: task_id})

        self.backend.revoke_extant_task(task_key)
        self.assertEqual(self.backend.tasks, {})

    def test_revoke_inexistant(self):
        # Note: this test also validates that we handle revoking inexistant
        # tasks without any exception:
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'

        self.assertEqual(self.backend.tasks, {})
        self.backend.revoke_extant_task(task_key)
        self.assertEqual(self.backend.tasks, {})


class InMemoryBackendGetTaskIdTestCase(InMemoryBackendTestCase):

    def test_get_existing(self):
        task_id = '6bf813e5-74aa-4f12-a308-7e0a4bec5916'
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'

        self.backend.tasks[task_key] = task_id
        self.assertEqual(self.backend.get_task_id(task_key), task_id)

    def test_get_inexisting(self):
        task_key = '5jPpAKs0HOzdBjAZud6yQiL'

        self.assertEqual(self.backend.tasks, {})
        self.assertIsNone(self.backend.get_task_id(task_key))
