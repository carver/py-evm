from lru import LRU

import rlp

from evm.db.backends.base import BaseDB

unset = object()


class CachedRLPDB(BaseDB):
    """
    Set and get decoded RLP objects, where the underlying db stores
    encoded objects.
    """
    def __init__(self, db, sedes, default_result=unset, cache_size=2048):
        self._sedes = sedes
        self._db = db
        self._default_result = default_result
        self._cached_objects = LRU(cache_size)
        # Use lru-dict instead of functools.lru_cache because the latter doesn't let us
        # invalidate a single entry, so we'd have to invalidate the whole cache in
        # _set_account() and that turns out to be too expensive.

    def __getitem__(self, key):
        if key not in self._cached_objects:
            self._cached_objects[key] = self._get_item_uncached(key)
        return self._cached_objects[key]

    def _get_item_uncached(self, key):
        encoded_object = self._db.get(key, b'')
        if encoded_object == b'':
            if self._default_result is unset:
                raise
            else:
                return self._default_result
        else:
            return rlp.decode(encoded_object, sedes=self._sedes)

    def __setitem__(self, key, rlp_object):
        self._cached_objects[key] = rlp_object

        encoded_object = rlp.encode(rlp_object, sedes=self._sedes)
        self._db[key] = encoded_object

    def __delitem__(self, key):
        del self._db[key]
