import jsonpickle
from cache.cache import Cache

class CacheProvider():
    def __init__(self, connection: Cache):
        self.connection = connection

    def get_from_cache(self, key):
        return self.__common__reader__(key)

    def __common__reader__(self, key):
        result = self.connection.get(key)
        if result is None:
            return None
        result = jsonpickle.decode(result)

        return result
    def __common__list__reader__(self, key, source):
        result = self.connection.get_list(key)
        if result is None:
            return None
        # result = [str(jsonpickle.decode(i)) for i in result]
        if source in result:
            return True
        else:
            return False

    def write_to_cache(self, key, source):
        # key = f"{self.context.tenant_context.tenant_id}-file-id"
        return self.__common__writer__(key, source)

    def __common__writer__(self, key, source):
        source = jsonpickle.encode(source, unpicklable=False)
        return self.connection.set(key, source, skip_memory_cache=True)

    def delete_key(self, key):
        return self.connection.delete(key)

    def delete_multi_keys(self, pattern):
        return self.connection.delete_keys(pattern=pattern)
