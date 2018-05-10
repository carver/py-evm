from abc import (
    ABCMeta,
    abstractmethod
)
from cytoolz import (
    compose,
)
import logging
from typing import (  # noqa: F401
    Type,
    TYPE_CHECKING
)

from trie import HexaryTrie

from evm.constants import (
    MAX_PREV_HEADER_DEPTH,
)
from evm.db.account import (  # noqa: F401
    BaseAccountDB,
    AccountDB,
)
from evm.db.hash_trie import (
    HashTrie,
)
from evm.db.journal import (
    JournalDB,
)
from evm.utils.datatypes import (
    Configurable,
)

if TYPE_CHECKING:
    from evm.computation import (  # noqa: F401
        BaseComputation,
    )
    from evm.vm.transaction_context import (  # noqa: F401
        BaseTransactionContext,
    )


class BaseState(Configurable, metaclass=ABCMeta):
    """
    The base class that encapsulates all of the various moving parts related to
    the state of the VM during execution.
    Each :class:`~evm.vm.base.BaseVM` must be configured with a subclass of the
    :class:`~evm.vm.state.BaseState`.

      .. note::

        Each :class:`~evm.vm.state.BaseState` class must be configured with:

        - ``computation_class``: The :class:`~evm.vm.computation.BaseComputation` class for
          vm execution.
        - ``transaction_context_class``: The :class:`~evm.vm.transaction_context.TransactionContext`
          class for vm execution.
    """
    #
    # Set from __init__
    #
    _chaindb = None
    execution_context = None
    root = None

    computation_class = None  # type: Type[BaseComputation]
    transaction_context_class = None  # type: Type[BaseTransactionContext]
    account_db_class = None  # type: Type[BaseAccountDB]
    transaction_executor = None  # type: Type[BaseTransactionExecutor]

    trie_class = compose(HashTrie, HexaryTrie)

    def __init__(self, db, execution_context, state_root):
        self._db = JournalDB(db)
        self.execution_context = execution_context
        self._trie = self.trie_class(self._db)
        self._trie.root_hash = state_root
        self.account_db = self._build_account_db()

    def _build_account_db(self):
        return self.get_account_db_class()(self._trie, self._db)

    #
    # Logging
    #
    @property
    def logger(self):
        return logging.getLogger('evm.vm.state.{0}'.format(self.__class__.__name__))

    #
    # Block Object Properties (in opcodes)
    #

    @property
    def coinbase(self):
        """
        Return the current ``coinbase`` from the current :attr:`~execution_context`
        """
        return self.execution_context.coinbase

    @property
    def timestamp(self):
        """
        Return the current ``timestamp`` from the current :attr:`~execution_context`
        """
        return self.execution_context.timestamp

    @property
    def block_number(self):
        """
        Return the current ``block_number`` from the current :attr:`~execution_context`
        """
        return self.execution_context.block_number

    @property
    def difficulty(self):
        """
        Return the current ``difficulty`` from the current :attr:`~execution_context`
        """
        return self.execution_context.difficulty

    @property
    def gas_limit(self):
        """
        Return the current ``gas_limit`` from the current :attr:`~transaction_context`
        """
        return self.execution_context.gas_limit

    #
    # Access to account db
    #
    @classmethod
    def get_account_db_class(cls):
        """
        Return the :class:`~evm.db.account.BaseAccountDB` class that the
        state class uses.
        """
        if cls.account_db_class is None:
            raise AttributeError("No account_db_class set for {0}".format(cls.__name__))
        return cls.account_db_class

    @property
    def root(self):
        """
        Return the current ``state_root`` from the underlying database
        """
        return self._trie.root_hash

    #
    # Journaling
    #
    def snapshot(self):
        """
        Perform a full snapshot of the current state.

        Snapshots are a combination of the :attr:`~root` at the time of the
        snapshot and the id of the changeset from the journaled DB.
        """
        return (self.root, self._db.record())

    def revert(self, snapshot):
        """
        Revert the VM to the state at the snapshot
        """
        state_root, changeset_id = snapshot

        # first revert the database state root.
        self._trie.root_hash = state_root
        # now roll the underlying database back
        self._db.discard(changeset_id)
        self.account_db = self._build_account_db()

    def commit(self, snapshot):
        """
        Commit the journal to the point where the snapshot was taken.  This
        will merge in any changesets that were recorded *after* the snapshot changeset.
        """
        _, checkpoint_id = snapshot
        self._db.commit(checkpoint_id)

    def persist(self) -> None:
        return self._db.persist()

    #
    # Access self.prev_hashes (Read-only)
    #
    def get_ancestor_hash(self, block_number):
        """
        Return the hash for the ancestor block with number ``block_number``.
        Return the empty bytestring ``b''`` if the block number is outside of the
        range of available block numbers (typically the last 255 blocks).
        """
        ancestor_depth = self.block_number - block_number - 1
        is_ancestor_depth_out_of_range = (
            ancestor_depth >= MAX_PREV_HEADER_DEPTH or
            ancestor_depth < 0 or
            ancestor_depth >= len(self.execution_context.prev_hashes)
        )
        if is_ancestor_depth_out_of_range:
            return b''
        ancestor_hash = self.execution_context.prev_hashes[ancestor_depth]
        return ancestor_hash

    #
    # Computation
    #
    def get_computation(self, message, transaction_context):
        """
        Return a computation instance for the given `message` and `transaction_context`
        """
        if self.computation_class is None:
            raise AttributeError("No `computation_class` has been set for this State")
        else:
            computation = self.computation_class(self, message, transaction_context)
        return computation

    #
    # Transaction context
    #
    @classmethod
    def get_transaction_context_class(cls):
        """
        Return the :class:`~evm.vm.transaction_context.BaseTransactionContext` class that the
        state class uses.
        """
        if cls.transaction_context_class is None:
            raise AttributeError("No `transaction_context_class` has been set for this State")
        return cls.transaction_context_class

    #
    # Execution
    #
    def apply_transaction(
            self,
            transaction):
        """
        Apply transaction to the vm state

        :param transaction: the transaction to apply
        :return: the new state root, and the computation
        """
        computation = self.execute_transaction(transaction)
        self.persist()
        return self.root, computation

    def get_transaction_executor(self):
        return self.transaction_executor(self)

    @abstractmethod
    def execute_transaction(self):
        raise NotImplementedError()


class BaseTransactionExecutor(metaclass=ABCMeta):
    def __init__(self, vm_state):
        self.vm_state = vm_state

    def get_transaction_context(self, transaction):
        return self.vm_state.get_transaction_context_class()(
            gas_price=transaction.gas_price,
            origin=transaction.sender,
        )

    def __call__(self, transaction):
        valid_transaction = self.validate_transaction(transaction)
        message = self.build_evm_message(valid_transaction)
        computation = self.build_computation(message, valid_transaction)
        finalized_computation = self.finalize_computation(valid_transaction, computation)
        return finalized_computation

    @abstractmethod
    def validate_transaction(self):
        raise NotImplementedError

    @abstractmethod
    def build_evm_message(self):
        raise NotImplementedError()

    @abstractmethod
    def build_computation(self):
        raise NotImplementedError()

    @abstractmethod
    def finalize_computation(self):
        raise NotImplementedError()
