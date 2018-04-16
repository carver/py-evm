from eth_utils import (
    keccak,
)


class FrozenHashTrie:
    _trie = None

    def __init__(self, trie):
        self._trie = trie

    def set(self, key, value):
        return self._trie.set(keccak(key), value)

    def delete(self, key):
        return self._trie.delete(keccak(key))

    def __getitem__(self, key):
        return self._trie[keccak(key)]

    def __contains__(self, key):
        return keccak(key) in self._trie

    @property
    def root_hash(self):
        return self._trie.root_hash


class HashTrie(FrozenHashTrie):
    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        return self.delete(key)

    @FrozenHashTrie.root_hash.setter
    def root_hash(self, value):
        self._trie.root_hash = value

    def set(self, key, value):
        delta = super().set(key, value)
        self._trie = self._trie.after(delta)
        return delta

    def delete(self, key):
        delta = super().delete(key)
        self._trie = self._trie.after(delta)
        return delta
