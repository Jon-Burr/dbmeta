from .store import Store, SeqStore, AssocStore, MutableSeqStore, MutableAssocStore
from .column import read_identity
from future.utils import iteritems

class TupleStore(Store):
    """ Store that stores data internally as namedtuples """
    def __init__(self, data=None, store_type=None, **kwargs):
        super(TupleStore, self).__init__(**kwargs)
        if data is not None:
            self.from_dict(data, store_type)

    def _dict_to_tuple(self, data):
        """ Read a tuple from a dictionary """
        return tuple(
                read_identity(c.name, data, c._desc.default, None)
                for c in self._columns)
        

    def _remote_to_tuple(self, data, store_type):
        """ Read a tuple from remote store data """
        return tuple(c.read_from(data, store_type) for c in self._columns)

    def _remote_from_tuple(self, tup, store_type):
        """ Convert a tuple to a dictionary for sending to a remote store """
        data = {}
        for c in self._columns:
            c.write_to(tup[c.index], data, store_type)
        return data

    def __getitem__(self, idx_pair):
        row_idx, col_idx = idx_pair
        return self._data[row_idx][col_idx]

    def __len__(self):
        return len(self._data)

class TupleSeqStore(TupleStore, SeqStore):
    """ Sequential store that stores data internally as namedtuples """
    def __init__(self, **kwargs):
        """ Create the store """
        self._data = []
        super(TupleSeqStore, self).__init__(**kwargs)

    def from_remote(self, data, store_type):
        """ Update the internal data store from the supplied remote store data """
        self._data = [self._remote_to_tuple(d, store_type) for d in data]

    def to_remote(self, store_type):
        """ Convert the internal data store to a tuple of dicts """
        return tuple(self._remote_from_tuple(t, store_type) for t in self._data)

class MutableTupleSeqStore(TupleSeqStore, MutableSeqStore):
    """ Mutable sequential store that stores data internally as namedtuples """

    def append(self, row_data):
        self._data.append(self._dict_to_tuple(row_data))

    def __setitem__(self, idx_pair, value):
        row_idx, col_idx = idx_pair
        self._data[row_idx] = tuple(
                value if i == col_idx else v
                for (i, v) in enumerate(self._data[row_idx]))

    def __delitem__(self, row_idx):
        del self._data[row_idx]
        MutableSeqStore.__delitem__(self, row_idx)

class TupleAssocStore(TupleStore, AssocStore):
    """ Associative store that stores data internally as namedtuples """
    def __init__(self, **kwargs):
        """ Create the store """
        self._data = {}
        super(TupleAssocStore, self).__init__(**kwargs)

    def from_dict(self, data, store_type):
        self._data = {
                self._index_column.read_func(k, store_type):
                self._remote_to_tuple(v, store_type)
                for k, v in iteritems(data)}

    def to_dict(self, store_type):
        return {
                self._index_column.write_func(k, store_type): \
                        self._remote_from_tuple(t, store_type)
                for k, t in iteritems(self._data)}

    def __iter__(self):
        return iter(self._data)

    def __contains__(self, row_idx):
        return row_idx in self._data

class MutableTupleAssocStore(TupleAssocStore, MutableAssocStore):
    """ Mutable associative store that stores data internally as namedtuples """

    def add(self, index, row_data):
        if index in self:
            raise KeyError(
                    "Attempting to add pre-existing index {0}!".format(index) )
        self._data[index] = self._dict_to_tuple(row_data)

    def __setitem__(self, idx_pair, value):
        row_idx, col_idx = idx_pair
        self._data[row_idx] = tuple(
                value if i == col_idx else v
                for (i, v) in enumerate(self._data[row_idx]))

    def __delitem__(self, row_idx):
        del self._data[row_idx]
