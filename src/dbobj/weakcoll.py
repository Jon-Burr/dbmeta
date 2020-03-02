import weakref
from future.utils import itervalues, iteritems

class WeakColl(object):
    """ An iterable collection of weakrefs to objects

        When iterating over these the actual objects are returned if the refs
        are still alive, otherwise the reference is removed from the list.

        This is not particularly carefully optimised...
    """

    def __init__(self):
        # Mapping of object IDs to their references
        self._refs = {}

    def flush(self, r=None):
        """ Remove any dead references """
        self._refs = {id(r()): r for r in itervalues(self._refs) if r() is not None}

    def __len__(self):
        """ Get the number of still living references """
        self.flush()
        return len(self._refs)

    def __iter__(self):
        """ Iterate over any still living referenced objects """
        self.flush()
        return (r() for r in itervalues(self._refs))

    def __contains__(self, obj):
        """ Is an object in the collection """
        return id(obj) in self._refs

    def remove(self, obj, permissive=True):
        """ Remove all references to an object from the collection
        
            A KeyError will only be raised if permissive is False
        """
        try:
            del self._refs[id(obj)]
        except KeyError:
            if not permissive:
                raise

    def rm_by_ref(self, r):
        """ Remove by the reference value.
        
            This should only be used by the weakref callback.
        """
        try:
            obj_id = next(k for (k, v) in iteritems(self._refs) if v is r)
        except StopIteration:
            return
        del self._refs[obj_id]

    def append(self, obj):
        """ Add an object to the collection (pass the actual object in here, not
            a weakref)
        """
        if obj not in self:
            self._refs[id(obj)] = weakref.ref(obj, self.rm_by_ref)
