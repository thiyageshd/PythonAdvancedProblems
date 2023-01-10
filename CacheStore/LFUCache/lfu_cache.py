from enum import Enum
import typing
from cachetools import LFUCache
import jsonpickle

def make_hash_from_value(data):
    import hashlib
    md5_object = hashlib.sha512()
    md5_object.update(data.encode('utf-8'))
    md5_hash = md5_object.hexdigest()
    return md5_hash

class ValueFoundIn(Enum):
    IN_MEMORY = 0
    AFTER_REGISTRATION = 1

class LFUCollector():
    def __init__(self):
        self.learning_dict = LFUCache(maxsize=20)
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