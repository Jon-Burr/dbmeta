from .db_object import DBObject
import collections
import abc
from future.utils import PY3, itervalues
if PY3:
    from collections.abc import MutableMapping
else:
    from collections import MutableMapping

class RemoteDatabase(MutableMapping):
    """ A database that is situated remotely, meaning that requests for
        information will be expensive and should be batched
    """

    @abc.abstractmethod
    def pull_updates(self, all_objs=False):
        """ Pull all updates in from the remote server
        
            If all_objs is True, pull updates for all known objects, not just
            the ones requesting it
        """
        pass

    @property
    def update_requested(self):
        """ Are any updates requested? """
        return any(obj.update_requested for obj in itervalues(self) )

    @property
    def _objects_to_update(self):
        """ The objects that should be updated """
        return (x for x in itervalues(self) if x.update_requested)

class WriteableRemoteDatabase(RemoteDatabase):
    """ A remote database that can be written to """
    @abc.abstractmethod
    def push_updates(self):
        """ Push all updates to the remote server """
        pass

class remotefield(property):
    """ Property class that describes a database field
    
        Has an extra 'field' attribute which is the name of the field in the
        database
    """
    def __init__(
            self, db_key, doc=None, backing=None, writeable=False,
            default=None):
        """ Create the property
        
            parameters:
                db_key: The name of the corresponding field in the database
                doc: The description of the field
                backing: The name of the private field. If not set, will be
                '_'+db_key
                writeable: Whether users should be able to modify this value

        """
        self.db_key = db_key
        if doc is None:
            doc = "{0} field".format(db_key)
        if backing is None:
            backing = "_{0}".format(db_key)
        self.backing = backing
        property.__init__(
                self,
                fget=type(self).make_getter(backing),
                fset=type(self).make_setter(backing) if writeable else None,
                doc=doc)

    @classmethod
    def make_getter(cls, backing_field):
        def fget(self):
            if self.update_requested:
                self.database.pull_updates()
            return getattr(self, backing_field)
        return fget

    @classmethod
    def make_setter(cls, backing_field):
        def fset(self, value):
            setattr(self, backing_field, value)
        return fset

    def _set_private(self, obj, value):
        """ The private setter method

            Meant to be used by the database methods
        """
        setattr(obj, self.backing, value)

def rwremotefield(db_key, doc=None, backing=None):
    return remotefield(db_key, doc, backing, writeable=True)

class RemoteDBObject(DBObject):
    """ An object situated in a remote database """

    def __init__(self, index, db):
        DBObject.__init__(self, index, db)
        self.request_update()

    @classmethod
    def _fields(cls):
        """ Any remoteproperties declared on this class """
        return ((a, x) for (a, x) in ((a, getattr(cls, a)) for a in dir(cls) )
                if isinstance(x, remotefield) )

    def force_update(self):
        """ Force an update of this object (will also pull all requested updates
            for the whole database
        """
        self.request_update()
        self.database.pull_updates()

    def request_update(self):
        """ Request an update from the remote db """
        self._update_requested = True

    @property
    def update_requested(self):
        """ Is an update requested """
        return self._update_requested

    def _load(self, **kwargs):
        """ Load in information provided by the remote database

            Sets values associated to any remotefields. Any extra work should be
            done by the base class, which should make sure to either call this
            function or set _update_requested to False
        """
        self._update_requested = False
        for _, f in type(self)._fields():
            if f.db_key in kwargs:
                f._set_private(self, kwargs[f.db_key])

