from abc import (
    ABCMeta,
    abstractmethod
)
from cytoolz import (
    compose,
)

import rlp

from trie import (
    HexaryTrie,
)

from eth_hash.auto import keccak

from evm.constants import (
    BLANK_ROOT_HASH,
    EMPTY_SHA3,
)
from evm.db.backends.base import (
    BaseDB,
)
from evm.db.cached_rlp import (
    CachedRLPDB,
)
from evm.db.hash_trie import (
    HashTrie,
)
from evm.rlp.accounts import (
    Account,
)
from evm.validation import (
    validate_is_bytes,
    validate_uint256,
    validate_canonical_address,
)

from evm.utils.numeric import (
    int_to_big_endian,
)
from evm.utils.padding import (
    pad32,
)


class BaseAccountDB(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self) -> None:
        raise NotImplementedError(
            "Must be implemented by subclasses"
        )

    #
    # Storage
    #
    @abstractmethod
    def get_storage(self, address, slot):
        raise NotImplementedError("Must be implemented by subclasses")

    @abstractmethod
    def set_storage(self, address, slot, value):
        raise NotImplementedError("Must be implemented by subclasses")

    #
    # Balance
    #
    @abstractmethod
    def get_balance(self, address):
        raise NotImplementedError("Must be implemented by subclasses")

    @abstractmethod
    def set_balance(self, address, balance):
        raise NotImplementedError("Must be implemented by subclasses")

    def delta_balance(self, address, delta):
        self.set_balance(address, self.get_balance(address) + delta)

    #
    # Code
    #
    @abstractmethod
    def set_code(self, address, code):
        raise NotImplementedError("Must be implemented by subclasses")

    @abstractmethod
    def get_code(self, address):
        raise NotImplementedError("Must be implemented by subclasses")

    @abstractmethod
    def get_code_hash(self, address):
        raise NotImplementedError("Must be implemented by subclasses")

    @abstractmethod
    def delete_code(self, address):
        raise NotImplementedError("Must be implemented by subclasses")

    #
    # Account Methods
    #
    @abstractmethod
    def account_is_empty(self, address):
        raise NotImplementedError("Must be implemented by subclass")


storage_key_map = compose(pad32, int_to_big_endian)


# TODO rename to State
class AccountDB(BaseAccountDB):
    def __init__(self, trie_db: BaseDB, raw_db: BaseDB) -> None:
        self._accounts = CachedRLPDB(trie_db, Account, Account())
        self._storage_db = raw_db

        # storage_rlp_db = CachedRLPDB(trie_db, rlp.sedes.big_endian_int, 0)
        # self._storage_db = KeyMapDB(storage_rlp_db, storage_key_map)
        # TODO ^

        self._code_db = raw_db

    #
    # Storage
    #
    def get_storage(self, address, slot):
        validate_canonical_address(address, title="Storage Address")
        validate_uint256(slot, title="Storage Slot")

        account = self._accounts[address]
        storage = HashTrie(HexaryTrie(self._storage_db, account.storage_root))

        slot_as_key = pad32(int_to_big_endian(slot))

        if slot_as_key in storage:
            encoded_value = storage[slot_as_key]
            return rlp.decode(encoded_value, sedes=rlp.sedes.big_endian_int)
        else:
            return 0

    def set_storage(self, address, slot, value):
        validate_uint256(value, title="Storage Value")
        validate_uint256(slot, title="Storage Slot")
        validate_canonical_address(address, title="Storage Address")

        account = self._accounts[address]
        storage = HashTrie(HexaryTrie(self._storage_db, account.storage_root))

        slot_as_key = pad32(int_to_big_endian(slot))

        if value:
            encoded_value = rlp.encode(value)
            storage[slot_as_key] = encoded_value
        else:
            del storage[slot_as_key]

        self._accounts[address] = account.copy(storage_root=storage.root_hash)

    def delete_storage(self, address):
        validate_canonical_address(address, title="Storage Address")

        account = self._accounts[address]
        self._accounts[address] = account.copy(storage_root=BLANK_ROOT_HASH)

    #
    # Balance
    #
    def get_balance(self, address):
        validate_canonical_address(address, title="Storage Address")

        account = self._accounts[address]
        return account.balance

    def set_balance(self, address, balance):
        validate_canonical_address(address, title="Storage Address")
        validate_uint256(balance, title="Account Balance")

        account = self._accounts[address]
        self._accounts[address] = account.copy(balance=balance)

    #
    # Nonce
    #
    def get_nonce(self, address):
        validate_canonical_address(address, title="Storage Address")

        account = self._accounts[address]
        return account.nonce

    def set_nonce(self, address, nonce):
        validate_canonical_address(address, title="Storage Address")
        validate_uint256(nonce, title="Nonce")

        account = self._accounts[address]
        self._accounts[address] = account.copy(nonce=nonce)

    def increment_nonce(self, address):
        current_nonce = self.get_nonce(address)
        self.set_nonce(address, current_nonce + 1)

    #
    # Code
    #
    def get_code(self, address):
        validate_canonical_address(address, title="Storage Address")

        try:
            return self._code_db[self.get_code_hash(address)]
        except KeyError:
            return b""

    def set_code(self, address, code):
        validate_canonical_address(address, title="Storage Address")
        validate_is_bytes(code, title="Code")

        account = self._accounts[address]

        code_hash = keccak(code)
        self._code_db[code_hash] = code
        self._accounts[address] = account.copy(code_hash=code_hash)

    def get_code_hash(self, address):
        validate_canonical_address(address, title="Storage Address")

        account = self._accounts[address]
        return account.code_hash

    def delete_code(self, address):
        validate_canonical_address(address, title="Storage Address")

        account = self._accounts[address]
        self._accounts[address] = account.copy(code_hash=EMPTY_SHA3)

    #
    # Account Methods
    #
    def account_has_code_or_nonce(self, address):
        return self.get_nonce(address) != 0 or self.get_code_hash(address) != EMPTY_SHA3

    def delete_account(self, address):
        validate_canonical_address(address, title="Storage Address")

        del self._accounts[address]

    def account_exists(self, address):
        validate_canonical_address(address, title="Storage Address")

        return bool(self._accounts[address])

    def touch_account(self, address):
        validate_canonical_address(address, title="Storage Address")

        account = self._accounts[address]
        self._accounts[address] = account

    def account_is_empty(self, address):
        return not self.account_has_code_or_nonce(address) and self.get_balance(address) == 0
