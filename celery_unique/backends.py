class BaseBackend:
    """
    Abstract reference backend.

    An abstract backend that defines the interface that other backends must
    implement.
    """

    def create_task_record(self, key, task_id, ttl):
        """
        Creates a new record for the recently-published unique task.

        :param str key: The unique key which identifies the task and its
            configuration.
        :param str task_id: The ID of the recently-published unique task.
        :param ttl: The TTL for the record, which should be (approximately)
            equal to the number of seconds remaining until the earliest time
            that the task is expected to be executed by Celery.
        """
        raise NotImplementedError()

    def get_task_id(self, key):
        """
        Returns the task_id for an exiting task
        """
        raise NotImplementedError()

    def revoke_extant_task(self, key):
        """
        Deletes a task for a given key.

        This deletes both the task and the cache entry.

        :param key: The string (potentially) used by the backend as the key for
            the record.
        :type redis_key: str | unicode
        """
        raise NotImplementedError()


class RedisBackend(BaseBackend):
    """
    A uniqueness backend that uses redis as a key/value store.

    See :class:`~.BaseBackend` for documentation on indivitual methods.
    """

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def get_task_id(self, key):
        task_id = self.redis_client.get(key)
        return task_id.decode() if task_id else None

    def revoke_extant_task(self, key):
        self.redis_client.delete(key)

    def create_task_record(self, key, task_id, ttl):
        self.redis_client.set(key, task_id, ex=ttl)


class InMemoryBackend(BaseBackend):
    """
    Dummy backend which uses an in-memory store.

    This is a dummy backend which uses an in-memory store. It is mostly
    suitable for development and testing, and should not be using in production
    environments.

    See :class:`~.BaseBackend` for documentation on indivitual methods.
    """

    def __init__(self):
        self.tasks = {}

    def get_task_id(self, key):
        return self.tasks.get(key, None)

    def revoke_extant_task(self, key):
        self.tasks.pop(key, None)

    def create_task_record(self, key, task_id, ttl=None):
        self.tasks[key] = task_id
