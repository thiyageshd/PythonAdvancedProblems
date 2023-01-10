import typing
import zlib
import redis
import logging
import time
from cachetools import LFUCache
from r2essentials.config import get_from_env
from threading import Thread
from enum import Enum

# from r2essentials.integrations.slack.rightrev import SlackNotifier

global ONE_TIME_LOG_PRINT
ONE_TIME_LOG_PRINT = []


def retry(times, exceptions):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    :param Exceptions: Lists of exceptions that trigger a retry attempt
    :type Exceptions: Tuple of Exceptions
    """
    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            excp = None
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logging.error(
                        'Exception thrown when attempting to run %s, attempt '
                        '%d of %d %s' % (func, attempt, times, get_from_env("REDIS_HOST", "localhost")), exc_info=e)
                    excp = e
                    time.sleep(2)
                    attempt += 1
            if attempt == 0 and excp is not None:
                logging.error(f"RedisRetryFailed {excp}")
                # SlackNotifier(None, "cache")\
                #     .notify_exception("RedisRetryFailed", {}, excp)
            return func(*args, **kwargs)
        return newfn
    return decorator


class ConsistencyType(Enum):
    EventualConsistency = 0
    HighlyConsistency = 1


class Cache(object):
    __slots__ = ('host', 'consistency', 'compress',
                 'read_host', 'write_host',
                 'max_connections', 'port', 'db',
                 'password', 'expiry', 'socket_timeout',
                 '_read_handle', '_write_handle')

    def __init__(self, host=None, port=6379,
                 db=0, password=None, socket_timeout=None, env_prefix="TENANT_CLONING", consistency=ConsistencyType.EventualConsistency):
        self.host = host
        self.consistency = consistency
        self.compress = True
        self.read_host = None
        self.write_host = None
        self.max_connections = int(get_from_env("CACHE_POOL_MAX", "10"))
        if host is None:
            self.host = self.get_node_check_prefix(env_prefix, 'REDIS_HOST')
        self.read_host = self.get_node_check_prefix(
            env_prefix, 'REDIS_READ_HOST')
        self.write_host = self.get_node_check_prefix(
            env_prefix, 'REDIS_WRITE_HOST')
        self.port = port
        self.db = db
        self.password = password
        self.expiry = int(get_from_env('CACHE_EXPIRY', str(60*60*2)))
        self.socket_timeout = socket_timeout
        self._read_handle = self.__open_read_session__()
        self._write_handle = self.__open_write_session__()

    def get_node_from_env(self, env_list: typing.List[str]) -> str:
        for entry in env_list:
            value = get_from_env(entry, "")
            if value != "":
                if entry not in ONE_TIME_LOG_PRINT:
                    logging.debug(f"Cache Node: {entry}={value}")
                    ONE_TIME_LOG_PRINT.append(entry)
                return value
        if not self.host:
            logging.warning(f"Environment Variable {entry} not set, so disabling cache")
        return self.host if self.host else "localhost"

    def get_node_check_prefix(self, prefix: str, target_env_name: str) -> str:
        return_list = []
        if prefix:
            if prefix.endswith("_"):
                prefix = f"{prefix}{target_env_name}"
            else:
                prefix = f"{prefix}_{target_env_name}"
            return_list = [prefix]
        # else:
        #     return_list = [self.host]
        return self.get_node_from_env(return_list)

    def close(self):
        try:
            self._write_handle.close()
        except Exception as error:
            logging.warning(
                "Error closing write handle connection", exc_info=error)
            pass
        try:
            self._read_handle.close()
        except Exception as error:
            logging.warning(
                "Error closing read handle connection", exc_info=error)
            pass

    def __enter__(self):
        return self

    def __del__(self):
        self.close()
        self.host = None
        try:
            self._write_handle.close()
        except:
            pass
        try:
            self._read_handle.client()
        except:
            pass

    def __exit__(self, type, value, traceback):
        self.__del__()

    @retry(times=3, exceptions=(Exception))
    def __open_read_session__(self):
        global _redis_pool_read
        _redis_pool_read = redis.ConnectionPool(host=self.read_host,
                                                port=self.port,
                                                password=self.password,
                                                db=self.db,
                                                max_connections=self.max_connections)
        client = redis.StrictRedis(connection_pool=_redis_pool_read)
        return client

    @retry(times=3, exceptions=(Exception))
    def __open_write_session__(self):
        global _redis_pool_write
        _redis_pool_write = redis.ConnectionPool(host=self.write_host,
                                                 port=self.port,
                                                 password=self.password,
                                                 db=self.db, max_connections=self.max_connections)
        client = redis.StrictRedis(connection_pool=_redis_pool_write)
        return client

    @retry(times=3, exceptions=(Exception))
    def extend_ttl(self, pattern):
        handle = self._read_handle
        if self.consistency == ConsistencyType.HighlyConsistency:
            handle = self._write_handle
        for key in handle.scan_iter(match=pattern):
            self._write_handle.expire(key, self.expiry)
        return True

    @retry(times=3, exceptions=(Exception))
    def set(self, key, value, skip_memory_cache=False):
        try:
            if value is None:
                return True  # we don't save values that are not
            if self.compress:
                value = zlib.compress(value.encode('utf-8'))
                self._write_handle.set(key, value, ex=self.expiry)
            else:
                self._write_handle.set(key, value, ex=self.expiry)
            self._write_handle.expire(key, self.expiry)
            return True
        except Exception as error:
            logging.error(f'Writing key {key} to cache error', exc_info=error)
            raise error

    @retry(times=3, exceptions=(Exception))
    def hset(self, name, key, value):
        self._write_handle.hset(name, key, value)
        self._write_handle.expire(key, self.expiry)
        return True

    @retry(times=3, exceptions=(Exception))
    def hgetall(self, key):
        handle = self._read_handle
        if self.consistency == ConsistencyType.HighlyConsistency:
            handle = self._write_handle
        return_data = handle.hgetall(key)
        self._write_handle.expire(key, self.expiry)
        return return_data

    @retry(times=3, exceptions=(Exception))
    def incr(self, key, value):
        self._write_handle.incr(key, value)
        self._write_handle.expire(key, self.expiry)
        return True

    @retry(times=3, exceptions=(Exception))
    def append_list(self, key, value):
        self._write_handle.sadd(key, value)
        self._write_handle.expire(key, self.expiry)
        return True

    @retry(times=3, exceptions=(Exception))
    def read_list(self, key):
        values = []
        handle = self._read_handle
        if self.consistency == ConsistencyType.HighlyConsistency:
            handle = self._write_handle
        return_data = handle.smembers(key)
        for i in return_data:
            if i is None:
                values.append(None)
            else:
                values.append(i.decode('utf-8'))
        values.sort()

        self._write_handle.expire(key, self.expiry)
        return values

    @retry(times=3, exceptions=(Exception))
    def get(self, key):
        handle = self._read_handle
        if self.consistency == ConsistencyType.HighlyConsistency:
            handle = self._write_handle
        result = handle.get(key)

        if result is None:
            return result

        self._write_handle.expire(key, self.expiry)

        if self.compress:
            result = zlib.decompress(result)
            return result.decode('utf-8')
        try:
            return result.decode('utf-8')
        except:
            return result

    @retry(times=3, exceptions=(Exception))
    def delete(self, key):
        result = self._write_handle.delete(key)
        return result == 1

    @retry(times=3, exceptions=(Exception))
    def get_values(self, pattern):
        handle = self._read_handle
        if self.consistency == ConsistencyType.HighlyConsistency:
            handle = self._write_handle
        keys = handle.keys(pattern=pattern)
        if keys is None:
            return keys

        # if self.compress:
        #     return [zlib.decompress(self.get(k)).decode('utf-8') for k in keys]
        return [self.get(k) for k in keys]

    @retry(times=3, exceptions=(Exception))
    def get_keys(self, pattern):
        handle = self._read_handle
        if self.consistency == ConsistencyType.HighlyConsistency:
            handle = self._write_handle
        keys = handle.keys(pattern=pattern)
        return keys

    @retry(times=3, exceptions=(Exception))
    def delete_keys(self, pattern):
        pipe = self._write_handle.pipeline()
        for key in self._write_handle.scan_iter(pattern):
            pipe.delete(key)
        pipe.execute()
        pipe.close()
        return True
