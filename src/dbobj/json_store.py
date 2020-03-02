""" Store classes for reading and writing to JSON """

from future.utils import iteritems
from .store import (
        Store, NamedTupSeqStore, NamedTupAssocStore, MutableNamedTupSeqStore,
        MutableNamedTupAssocStore)
import json

class NamedtupleEncoder(json.JSONEncoder):
    """ Encoder for namedtuple types """
    def default(self, o):
        print ("Encode: {0}".format(o) )
        if hasattr(o, "_asdict"):
            return super(NamedtupleEncoder, self).default(o._asdict() )
        else:
            return super(NamedtupleEncoder, self).default(o)

class JSONStore(Store):
    """ Immutable sequential JSON store """
    def __init__(self, ordered_columns, db_file, allow_missing=False):
        """ Create the store

            If allow_missing is True, then allow the file to be absent
        """
        self._db_file = db_file
        try:
            with open(db_file, 'r') as fp:
                data = json.load(fp)
        except IOError:
            if not allow_missing:
                raise
            elif self.is_associative:
                data = {}
            else:
                data = []
        super(JSONStore, self).__init__(ordered_columns, data)

class MutableJSONStore(JSONStore):
    """ Mutable sequential JSON store """
    def __init__(self, ordered_columns, db_file):
        """ Create the store """
        super(MutableJSONStore, self).__init__(
                ordered_columns=ordered_columns, db_file=db_file,
                allow_missing=True)

    def write(self):
        """ Write the store back to disk """
        with open(self._db_file, 'w') as fp:
            if self.is_associative:
                json.dump(
                        {k : v._asdict() for k, v in iteritems(self._data)},
                        fp)
            else:
                json.dump([v._asdict() for v in self._data], fp)

class JSONSeqStore(JSONStore, NamedTupSeqStore):
    pass

class MutableJSONSeqStore(MutableJSONStore, MutableNamedTupSeqStore):
    pass

class JSONAssocStore(JSONStore, NamedTupAssocStore):
    pass

class MutableJSONAssocStore(MutableJSONStore, MutableNamedTupAssocStore):
    pass
