from cytoolz import (
    curry,
)
import rlp
from trie import (
    HexaryTrie,
)

from evm.db.backends.memory import MemoryDB
from evm.db.chain import ChainDB


def trie_deltas_for_serializables(trie, serializables):
    for index, rlp_object in enumerate(serializables):
        index_key = rlp.encode(index, sedes=rlp.sedes.big_endian_int)
        yield trie.set(index_key, rlp.encode(rlp_object))


@curry
def build_new_trie_delta(trie_class, serializables):
    empty_trie = trie_class(db={})
    deltas = trie_deltas_for_serializables(empty_trie, serializables)
    return trie_class.Delta.join(deltas, empty_trie.root_hash)
