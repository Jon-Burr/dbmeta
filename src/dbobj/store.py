""" Database store classes

    The store is the object actually holding the data.

    Stores can be mutable/immutable and sequential/associative.

    A mutable store allows adding or removing rows, and changing information in
    a given row, while an immutable store is read-only. Note that if a mutable
    type such as a list or a dictionary is returned from the the store it will
    be mutable. This is not desirable even in the case of a mutable store, as
    the store will not be aware that the value has changed and therefore may not
    trigger some necessary action, such as writing the change to disk.

    The index in a sequential store should hold no true information (beyond the
    relation between an object and its data) so this information can be changed
    'under the hood' without causing any problems. This store is akin to a list
    or tuple.

    However, the index in an associative store holds meaningful information (and
    should uniquely identify the row). This store is most akin to a dict.
"""
from builtins import object
from future.utils import PY3, iteritems, with_metaclass
if PY3:
    from collections.abc import Sequence, Mapping
else:
    from collections import Sequence, Mapping
from collections import namedtuple
import abc
import json
from .weakcoll import WeakColl

class Store(with_metaclass(abc.ABCMeta, object)):
    """ Base class for all store objects 
    
        A store keeps references to any databases that reference it. This will
        usually be just one, but the possibility exists for it to be more

        Stores should be immutable, unless they explicitly inherit from a
        Mutable base class. However, we want there to be a sensible error
        message when trying to modify one, so mutable methods are defined here,
        and then overridden with abstract methods in the mutable base classes
    """

    def __init__(self):
        self._db_refs = WeakColl()

    def add_store_ref(self, store):
        """ Add a store to our reference lists """
        self._db_refs.append(store)

    def rm_store_ref(self, store):
        """ Remove a store to our reference lists """
        self._db_refs.remove(store)

    @abc.abstractmethod
    def __getitem__(self, idx_pair):
        """ Retrieve a value corresponding to a row+index pair """
        pass

    def __setitem__(self, idx_pair, value):
        """ Throw an error when trying to mutate an immutable object """
        raise ValueError("Attempting to modify immutable store!")

    def __delitem__(self, idx):
        """ Throw an error when trying to mutate an immutable object """
        raise ValueError("Attempting to modify immutable store!")

    @abc.abstractmethod
    def __len__(self):
        """ The number of *rows* in this store """
        pass

class SeqStore(Store):
    """ Base class for sequential stores """

    @property
    def is_sequential(self):
        return True

    @property
    def is_associative(self):
        return False

    @property
    def is_mutable(self):
        return False

    def append(self, row_data):
        """ Throw an error when trying to mutate an immutable object """
        raise ValueError("Attempting to modify immutable store!")

class AssocStore(Store):
    """ Base class for associative stores """

    @property
    def is_sequential(self):
       return False

    @property
    def is_associative(self):
        return True

    @property
    def is_mutable(self):
        return True

    def add(self, index, row_data):
        """ Throw an error when trying to mutate an immutable object """
        raise ValueError("Attempting to modify immutable store!")

    @abc.abstractmethod
    def __iter__(self):
        """ Iterate over the keys held in this store """
        pass

    @abc.abstractmethod
    def __contains__(self, row_idx):
        """ True if this index is in this store """
        pass

class MutableSeqStore(SeqStore):
    """ Base class for mutable sequential stores """

    @property
    def is_mutable(self):
        return True

    @abc.abstractmethod
    def append(self, row_data):
        """ Add a new row to the store

            This differs from __setitem__ as that is used to set a row/column
            pair.

            Row_data should contain the data with which to populate the row
        """
        pass

    @abc.abstractmethod
    def __setitem__(self, idx_pair, value):
        """ Set a value corresponding to a row+index pair """
        pass

    @abc.abstractmethod
    def __delitem__(self, row_idx):
        """ Delete a whole row

            The implementation here is responsible for remapping the indices of
            all rows past the deleted one and should be called in most derived
            implementations. Note that it should be called *after* the deletion
            has been done (it assumes that len returns the length after
            deletion)
        """
        remap = {idx + 1: idx for idx in range(row_idx, len(self) )}
        if not remap:
            return
        for db in self._db_refs:
            db._remap_indices(remap)


class MutableAssocStore(AssocStore):
    """ Base class for mutable associative stores """

    @property
    def is_mutable(self):
        return True

    @abc.abstractmethod
    def add(self, index, row_data):
        """ Add a new row with associated index to this store

            This differs from __setitem__ as that is used to set a row/column
            pair. Will throw an error if the item already exists
        """
        pass

    @abc.abstractmethod
    def __setitem__(self, idx_pair, value):
        """ Set a value corresponding to a row+index pair """
        pass

    @abc.abstractmethod
    def __delitem__(self, row_idx):
        """ Delete a whole row """
        pass

class NamedTupSeqStore(SeqStore):
    """ Sequential store that stores data internally as namedtuples """
    def __init__(self, ordered_columns, data = None):
        """ Create the store.

            Parameters:
                ordered_columns: The names of the columns in stores, in the
                                 correct order
                data: The data to be stored, should be an iterable of dicts
        """
        self._tuple_cls = namedtuple("Storage", ordered_columns)
        self._data = None
        if data is not None:
            self._data = [self._tuple_cls(**d) for d in data]
        super(NamedTupSeqStore, self).__init__()

    def __getitem__(self, idx_pair):
        row_idx, col_idx = idx_pair
        return self._data[row_idx][col_idx]

    def __len__(self):
        return len(self._data)

class MutableNamedTupSeqStore(NamedTupSeqStore, MutableSeqStore):
    """ Mutable sequential store that stores data internall as namedtuples """

    def append(self, row_data):
        self._data.append(self._tuple_cls(**row_data) )

    def __setitem__(self, idx_pair, value):
        row_idx, col_idx = idx_pair
        self._data[row_idx] = self._data[row_idx]._replace(
                {self._tuple_cls._fields[col_idx] : value})

    def __delitem__(self, row_idx):
        del self._data[row_idx]
        MutableSeqStore.__delitem__(self, row_idx)

class NamedTupAssocStore(AssocStore):
    """ Associative store that stores data internally as namedtuples """
    def __init__(self, ordered_columns, data = None):
        """ Create the store.

            Parameters:
                ordered_columns: The names of the columns in stores, in the
                                 correct order
                data: The data to be stored, should be an iterable of dicts
        """
        self._tuple_cls = namedtuple("Storage", ordered_columns)
        self._data = None
        if data is not None:
            self._data = {
                    k: self._tuple_cls(**v) for k, v in iteritems(data)}
        super(NamedTupAssocStore, self).__init__()

    def __getitem__(self, idx_pair):
        row_idx, col_idx = idx_pair
        return self._data[row_idx][col_idx]

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __contains__(self, row_idx):
        return row_idx in self._data

class MutableNamedTupAssocStore(NamedTupAssocStore, MutableAssocStore):
    """ Mutable associative store that stores data internally as namedtuples """

    def add(self, index, row_data):
        if index in self:
            raise KeyError(
                    "Attempting to add pre-existing index {0}!".format(index) )
        print(row_data)
        self._data[index] = self._tuple_cls(**row_data)

    def __setitem__(self, idx_pair, value):
        row_idx, col_idx = idx_pair
        self._data[row_idx] = self._data[row_idx]._replace(
                {self._tuple_cls._fields[col_idx] : value})

    def __delitem__(self, row_idx):
        del self._data[row_idx]
