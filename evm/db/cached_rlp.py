import rlp

from evm.db.backends.base import BaseDB


unset = object()


# TODO add lru cache
class CachedRLPDB(BaseDB):
    """
    Set and get decoded RLP objects, where the underlying db stores
    encoded objects.
    """
    def __init__(self, db, sedes, default_result=unset):
        self._sedes = sedes
        self._db = db
        self._default_result = default_result

    def __getitem__(self, key):
        encoded_object = self._db.get(key, b'')
        if encoded_object == b'':
            if self._default_result is unset:
                raise
            else:
                return self._default_result
        else:
            return rlp.decode(encoded_object, sedes=self._sedes)

    def __setitem__(self, key, rlp_object):
        encoded_object = rlp.encode(rlp_object, sedes=self._sedes)
        self._db[key] = encoded_object

    def __delitem__(self, key):
        del self._db[key]
