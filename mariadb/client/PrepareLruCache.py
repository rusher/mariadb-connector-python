from collections import OrderedDict

from mariadb.message.server.CachedPrepareResultPacket import CachedPrepareResultPacket


class PrepareLruCache:

    __slots__ = ('cache', 'capacity')

    # initialising capacity
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    # we return the value of the key
    # that is queried in O(1) and return -1 if we
    # don't find the key in out dict / cache.
    # And also move the key to the end
    # to show that it was recently used.
    def get(self, key: str) -> CachedPrepareResultPacket:
        if key not in self.cache:
            return None
        else:
            self.cache.move_to_end(key)
            return self.cache[key]

    # first, we add / update the key by conventional methods.
    # And also move the key to the end to show that it was recently used.
    # But here we will also check whether the length of our
    # ordered dictionary has exceeded our capacity,
    # If so we remove the first key (least recently used)
    def put(self, key: str, value: CachedPrepareResultPacket) -> str:
        if key not in self.cache:
            self.cache[key] = value
            self.cache.move_to_end(key)
        else:
            #self.cache[key].incrementUse(prepared_statement)
            value.un_cache()
            return key

        if len(self.cache) > self.capacity:
            (removed_key, removed_value) = self.cache.popitem(last=False)
            removed_value.un_cache()

        return None
