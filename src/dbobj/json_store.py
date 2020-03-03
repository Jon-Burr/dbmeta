""" Store classes for reading and writing to JSON """

from future.utils import iteritems
from .store import Store
from .namedtup_store import (
        NamedTupSeqStore, NamedTupAssocStore, MutableNamedTupSeqStore,
        MutableNamedTupAssocStore)
import json
import jsonpatch
import os
import time
import logging
logger = logging.getLogger(__name__)

class JSONStore(Store):
    """ Immutable sequential JSON store """

    _store_type = "JSON"

    def __init__(self, db_file, allow_missing=False, **kwargs):
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
            else:
                data = None
        super(JSONStore, self).__init__(data=data, **kwargs)

    def update(self):
        """ Update our internal storage from the file on disk.

            If this is a sequential store it will almost certainly mess up any
            referenced rows
        """
        if not os.path.exists(self._db_file):
            return
        with open(self._db_file, 'r') as fp:
            self.from_dict(json.load(fp))

class MutableJSONStore(JSONStore):
    """ Mutable sequential JSON store """
    def __init__(self, db_file, **kwargs):
        """ Create the store """
        self._patches = []
        super(MutableJSONStore, self).__init__(
                db_file=db_file, allow_missing=True, **kwargs)

    def update(self, **kwargs):
        """ Update our internal storage from the file on disk.

            If this is a sequential store it will almost certainly mess up any
            referenced rows
        """
        if 'indent' not in kwargs:
            kwargs["indent"] = 2
        if not os.path.exists(self._db_file):
            # If the file doesn't exist then we don't need to do anything
            return
        # We have to try and patch the existing file
        with open(self._db_file, 'r') as fp:
            on_disk = json.load(fp)
        try:
            patch = jsonpatch.JsonPatch(self._patches)
            patch.apply(on_disk, in_place=True)
        except jsonpatch.JsonPatchException as e:
            # Use the current POSIX time stamp to make a unique filename
            stamp = int(time.time() )
            tmp_db = "{0}.{1}".format(self._db_file, stamp)
            tmp_patches = tmp_db + ".jsonpatch"

            logger.error((
                "Failed to apply patches! Will write current info in {0},"+
                "{1} files").format(tmp_db, tmp_patches))
            with open(tmp_db, 'w') as fp:
                json.dump(self.to_dict(), fp, **kwargs)


    def write(self, **kwargs):
        """ Write the store back to disk
        
            kwargs are forwarded back to the json.dump function
        """
        # First, attempt to update the local store
        self.update()
        # Only get here if the file doesn't already exist
        with open(self._db_file, 'w') as fp:
            json.dump(self.to_dict(), fp, **kwargs)

    def __setitem__(self, idx_pair, value):
        # Get the current value
        row_idx, col_idx = idx_pair
        before = self._from_tuple(self._data[row_idx])
        super(MutableJSONStore, self).__setitem__(idx_pair, value)
        # Use make_patch to patch the value, and prepend the path to each
        # operation
        after = self._from_tuple(self._data[row_idx])
        path = self._index_column.write_func(row_idx, self._store_type)
        self._patches += [{
            "op": o["op"], "value" : o["value"],
            "path": "/{0}{1}".format(path, "/"+o["path"] if o["path"] else "")}
            for o in jsonpatch.make_patch(current, value)]

class JSONSeqStore(JSONStore, NamedTupSeqStore):
    pass

class MutableJSONSeqStore(MutableJSONStore, MutableNamedTupSeqStore):
    def __delitem__(self, idx):
        super(MutableJSONSeqStore, self).__delitem__(idx)
        # The patch here first checks that the thing we're about to remove is
        # what we *expect* to remove. The reason to do this is make *very* sure
        # that we're removing the right thing
        # Convert the index if necessary
        if self.is_associative:
            idx = self._index_column.write_func(idx, self._store_type)
        self._patches.append({
            "op": "test", "path": "/{0}".format(idx),
            "value": self._data[idx]._asdict()})
        # Then the one that removes it
        self._patches.append({"op": "remove", "path": "/"+idx})

    def append(self, row_data):
        super(MutableJSONSeqStore, self).append(row_data)
        self._patches.append({
            "op": "add", "path": "/-",
            "value": self._from_tuple(self._data[-1])})
               

class JSONAssocStore(JSONStore, NamedTupAssocStore):
    pass

class MutableJSONAssocStore(MutableJSONStore, MutableNamedTupAssocStore):
    def __delitem__(self, idx):
        super(MutableJSONAssocStore, self).__delitem__(idx)
        self._patches.append({"op": "remove", "path" : "/"+idx})

    def add(self, index, row_data):
        super(MutableJSONAssocStore, self).add(index, row_data)
        write_index = self._index_column.write_func(index, self._store_type)
        self._patches.append({
            "op": "add", "path": "/{0}".format(write_index), 
            "value": self._from_tuple(self._data[index])})
