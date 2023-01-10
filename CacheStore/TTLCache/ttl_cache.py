from enum import Enum
import typing
from cachetools import TTLCache
import jsonpickle
from config import get_from_env

class ValueFoundIn(Enum):
    IN_MEMORY = 0
    AFTER_REGISTRATION = 1

class TTLcollector():
    def __init__(self):
        self.expiry_time = int(get_from_env("EXPIRY_WINDOW_FOR_TTL_HRS", "6"))
        self.max_size = int(get_from_env("MAX_ITEMS_IN_TTL_CACHE", "200"))
        self.learning_dict = TTLCache(maxsize=self.max_size, ttl= 60 * 60 * self.expiry_time)
        self.found_in = ValueFoundIn.AFTER_REGISTRATION

    def make_hashed_value(self, key_params_1, key_params_2) -> str:
        value = jsonpickle.encode({'key_params_1': key_params_1, 'key_params_2': key_params_2})
        return make_hash_from_value(value)

    def register_value(self, key_params_1, key_params_2, value, hashed_value: typing.Optional[str]):
        if hashed_value is None:
            hashed_value = self.make_hashed_value(key_params_1, key_params_2)
        self.learning_dict[hashed_value] = value

    def parsed_value(self, key_params_1, key_params_2):
        hashed_value = self.make_hashed_value(key_params_1, key_params_2)
        stored_value = self.learning_dict.get(hashed_value)
        if stored_value is None:
            value = True ###TODO - Get from the DB
            self.register_value(key_params_1, key_params_2, value, hashed_value)
            self.found_in = ValueFoundIn.AFTER_REGISTRATION
            return value
        self.found_in = ValueFoundIn.IN_MEMORY
        return stored_value

def make_hash_from_value(data):
    import hashlib
    md5_object = hashlib.sha512()
    md5_object.update(data.encode('utf-8'))
    md5_hash = md5_object.hexdigest()
    return md5_hash