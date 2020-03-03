from collections import namedtuple
from .store import Store, SeqStore, AssocStore, MutableSeqStore, MutableAssocStore
from future.utils import iteritems

class NamedTupStore(Store):
    """ Store that stores data internally as namedtuples """
    def __init__(self, data=None, **kwargs):
        super(NamedTupStore, self).__init__(**kwargs)
        self._tuple_cls = namedtuple(
                type(self).__name__,
                [c.name for c in self._columns])
        if data is not None:
            self.from_dict(data)

    def _to_tuple(self, data):
        """ Read a tuple from remote store data """
        return self._tuple_cls(**{
            c.name: c.read_from(data, self._store_type)
            for c in self._columns})

    def _from_tuple(self, tup):
        """ Convert a tuple to a dictionary for sending to a remote store """
        data = {}
        for c in self._columns:
            c.write_to(tup[c.index], data, self._store_type)
        return data

    def __getitem__(self, idx_pair):
        row_idx, col_idx = idx_pair
        return self._data[row_idx][col_idx]

    def __len__(self):
        return len(self._data)

class NamedTupSeqStore(NamedTupStore, SeqStore):
    """ Sequential store that stores data internally as namedtuples """
    def __init__(self, **kwargs):
        """ Create the store """
        self._data = []
        super(NamedTupSeqStore, self).__init__(**kwargs)

    def from_dict(self, data):
        """ Update the internal data store from the supplied data """
        self._data = [self._to_tuple(d) for d in data]

    def to_dict(self):
        """ Convert the internal data store to a tuple of dicts """
        return tuple(self._from_tuple(t) for t in self._data)

class MutableNamedTupSeqStore(NamedTupSeqStore, MutableSeqStore):
    """ Mutable sequential store that stores data internally as namedtuples """

    def append(self, row_data):
        self._data.append(self._to_tuple(row_data))

    def __setitem__(self, idx_pair, value):
        row_idx, col_idx = idx_pair
        col = self._columns[col_idx]
        self._data[row_idx] = self._data[row_idx]._replace(**{col.name : value})

    def __delitem__(self, row_idx):
        del self._data[row_idx]
        MutableSeqStore.__delitem__(self, row_idx)

class NamedTupAssocStore(NamedTupStore, AssocStore):
    """ Associative store that stores data internally as namedtuples """
    def __init__(self, **kwargs):
        """ Create the store """
        self._data = {}
        super(NamedTupAssocStore, self).__init__(**kwargs)

    def from_dict(self, data):
        self._data = {
                self._index_column.read_func(k, self._store_type):
                self._to_tuple(v)
                for k, v in iteritems(data)}

    def to_dict(self):
        return {
                self._index_column.write_func(k, self._store_type): \
                        self._from_tuple(t)
                for k, t in iteritems(self._data)}

    def __iter__(self):
        return iter(self._data)

    def __contains__(self, row_idx):
        return row_idx in self._data

class MutableNamedTupAssocStore(NamedTupAssocStore, MutableAssocStore):
    """ Mutable associative store that stores data internally as namedtuples """

    def add(self, index, row_data):
        index = self._index_column.read_func(index, self._store_type)
        if index in self:
            raise KeyError(
                    "Attempting to add pre-existing index {0}!".format(index) )

        self._data[index] = self._to_tuple(row_data)

    def __setitem__(self, idx_pair, value):
        row_idx, col_idx = idx_pair
        col = self._columns[col_idx]
        self._data[row_idx] = self._data[row_idx]._replace(**{col.name: value})

    def __delitem__(self, row_idx):
        del self._data[row_idx]
