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
from future.utils import PY3, with_metaclass
if PY3:
    from collections.abc import Sequence, Mapping
else:
    from collections import Sequence, Mapping
import abc
import json

class Store(with_metaclass(abc.ABCMeta, object)):
    """ Base class for all store objects 
    
        Stores should be immutable, unless they explicitly inherit from a
        Mutable base class. However, we want there to be a sensible error
        message when trying to modify one, so mutable methods are defined here,
        and then overridden with abstract methods in the mutable base classes
    """

    def __init__(self, db):
        self._db = db

    @property
    def _columns(self):
        """ The columns in this store """
        return self._db._columns

    @property
    def _index_column(self):
        """ The index column for this store

            Note that unlike for databases this returns the column itself, not
            just its name
        """
        return getattr(type(self._db), self._db._index_column)

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
        self._db._remap_indices(remap)


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
