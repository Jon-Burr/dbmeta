from __future__ import print_function
from future.utils import PY3, iteritems
from builtins import object, zip
from itertools import repeat
from inspect import isgenerator
from functools import wraps
import operator
if PY3:
    from collections.abc import Mapping
    from inspect import signature
else:
    from collections import Mapping
    from funcsigs import signature

class AlgebraicGenerator(object):
    def __init__(self, itr):
        self._itr = itr

    @classmethod
    def apply(cls, func, *args, **kwargs):
        """ Apply a function elementwise for iterables
        
            Any argument that is not an AlgebraicGenerator will be replaced by
            itertools.repeat. This means that if no arguments are generators,
            the returned generator will never be exhausted!
        """
        def to_repeat(x):
            if isinstance(x, cls) or isgenerator(x):
                return x
            else:
                return repeat(x)

        # Make any arguments that aren't generators into 'repeat' functions
        args = [to_repeat(a) for a in args]

        # Make any kwargs the same
        if kwargs:
            # zip the values together
            kwargs = {k : to_repeat(v) for k, v in iteritems(kwargs)}
            # and then zip them back together with the original keys
            g_kw = (dict(zip(kwargs.keys(), vs)) for vs in zip(*kwargs.values() ))
        else:
            g_kw = repeat({})
        args.append(g_kw)
        return cls(func(*a[:-1], **a[-1]) for a in zip(*args))

    def call(self, func, *args, **kwargs):
        """ Call the given function as a bound function on each of the members
            of this iterable

            args and kwargs are provided as arguments
        """
        args = (self,) + args
        return type(self).apply(func, *args, **kwargs)

    def __getattr__(self, name):
        """ Return an iterator getting the attribute over all elements """
        return self.call(getattr, name)

    def __call__(self, *args, **kwargs):
        """ If this is an iterable of callables, then call them elementwise with
            the provided arguments
        """
        def do_call(self, *args, **kwargs):
            return self(*args, **kwargs)
        return self.call(do_call, *args, **kwargs)

    def __iter__(self):
        return self._itr

    def __next__(self):
        return next(self._itr)

    def __eq__(self, other):
        return self.call(operator.eq, other)

    def __ne__(self, other):
        return self.call(operator.ne, other)

    def __gt__(self, other):
        return self.call(operator.gt, other)

    def __ge__(self, other):
        return self.call(operator.ge, other)

    def __le__(self, other):
        return self.call(operator.le, other)

    def __lt__(self, other):
        return self.call(operator.lt, other)

    def __add__(self, other):
        return self.call(operator.add, other)

    def __sub__(self, other):
        return self.call(operator.sub, other)

    def __mul__(self, other):
        return self.call(operator.mul, other)

    def __div__(self, other):
        return self.call(operator.div, other)

    def __abs__(self):
        return self.call(operator.abs)

    def __mod__(self, other):
        return self.call(operator.mod, other)

    def __and__(self, other):
        return self.call(operator.and_, other)

    def __or__(self, other):
        return self.call(operator.or_, other)

def identity(x):
    """ Helper identity function x -> x """
    return x

class ColumnDescBase(object):
    """ Describe columns and the index column in the database

        Column descriptions are mainly used by the metaclass - at class creation
        time it will replace them with an actual column type. The column type is
        decided as follows - first the description is checked to see if it has
        col_cls set, if that is None, then the class is checked for the _col_cls
        attribute (as are its bases in the correct mro). If no class is found
        this way, an error will be raised.
    """
    def __init__(
            self, doc=None, col_cls=None, type=identity, store_type=identity):
        """ Create the description

            Parameters:
                doc: The docstring for the column
                col_cls: The column class to be created from this description

            The doc string can be a format string, its format function will be
            called with keyword arguments name and index (the name and index of
            the column)

            Conversions
            -----------
            There are two conversion functions that can be supplied.
            type:
                Applied when reading a field directly from the database. This
                rarely needs to be anything other than identity but might be
                useful when overriding an existing column via inheritance
            store_type:
                Applied when writing a field directly back into the database,
                should be the inverse of type
        """
        if doc is None:
            doc = "The {name} column in the database"
        self.doc = doc
        self.col_cls = col_cls
        self.type = type
        self.store_type = store_type

class ColumnBase(property):
    """ Column base implementation

        A column should be a property that returns all the values for that
        column as an iterator but is not directly settable
    """
    def __init__(self, name, desc, fget, index=None):
        property.__init__(self, fget=fget)
        self.__doc__ = desc.doc.format(name=name, index=index)
        self._name = name
        self._desc = desc


    @property
    def name(self):
        """ The name of this column 
            
            The name should be the name of the attribute that this is accessed
            through on the database/row classes
        """
        return self._name


    @property
    def index(self):
        """ The index of this column """
        return self._index

    @property
    def type(self):
        """ The conversion from stored -> returned when getting """
        return self._desc.type

    @property
    def store_type(self):
        """ The conversion from set value -> stored when setting """
        return self._desc.store_type


def reader(f):
    """ Decorator that makes a type conversion function into a valid read_func
   
        If the provided function has a 'source' kwarg it the decorated function
        will use this and it will act differently on different store types,
        otherwise it will act the same for all sources
    """
    sig = signature(f)
    if "source" not in sig.parameters:
        # First create a new function that wraps f with an ignored source
        # parameter
        @wraps(f)
        def g(x, source):
            return f(x)
    else:
        g = f

    @wraps(g)
    def wrapper(key, dct, default, source):
        try:
            val = dct[key]
        except KeyError:
            if default == ColumnDesc.NO_DEFAULT:
                raise
            val = default
        return g(val, source=source)

    return wrapper

read_identity = reader(identity)


def writer(f):
    """ Decorator that makes a type conversion function into a valid write_func
   
        If the provided function has a 'target' kwarg it the decorated function
        will use this and it will act differently on different store types,
        otherwise it will act the same for all targets
    """
    sig = signature(f)
    if "target" not in sig.parameters:
        # First create a new function that wraps f with an ignored target
        # parameter
        @wraps(f)
        def g(x, target):
            return f(x)
    else:
        g = f

    @wraps(g)
    def wrapper(x, key, dct, target):
        dct[key] = g(x, target=target)

    return wrapper

write_identity = writer(identity)


class ColumnDesc(ColumnDescBase):
    """ Describe a column in the database
    
        Column descriptions are mainly used by the metaclass - at class creation
        time it will replace them with an actual column type. The column type is
        decided as follows - first the description is checked to see if it has
        col_cls set, if that is None, then the class is checked for the _col_cls
        attribute (as are its bases in the correct mro). If no class is found
        this way, an error will be raised.
    """
    NO_DEFAULT=object()
    def __init__(
            self, doc=None, key=None, col_cls=None, default=NO_DEFAULT,
            read_func=read_identity, write_func=write_identity,
            type=identity, store_type=identity):
        """ Create the description

            Parameters:
                doc: The docstring for the column
                key: The name of the column in remote stores
                col_cls: The column class to be created from this description
                default: The default value this column should take

            key should be a mapping from remote store type to the key of this
            column in that store, with None representing the default value. If a
            string is provided then key will be replaced internally by {None:
            key}. If no value is provided then key will be replaced internally
            with {None: name} where name is the attribute name of the column on
            the database class

            The doc string can be a format string, its format function will be
            called with keyword arguments name and index (the name and index of
            the column)

            Conversions
            -----------
            There are four conversion functions that can be supplied.
            type:
                Applied when reading a field directly from the database. This
                rarely needs to be anything other than identity but might be
                useful when overriding an existing column via inheritance
            store_type:
                Applied when writing a field directly back into the database,
                should be the inverse of type
            read_func:
                Applied when reading a field *into* the database from some
                remote store (e.g. a JSON file). In order to support multiple
                types of remote store, each remote store should be named to
                allow this function to behave differently for different type.
                This name is provided as the source keyword parameter.
            write_func:
                The inverse of read_func, applied when writing the field back
                into the remote store. For this function the remote store type
                is provided as the target keyword parameter. The function
                receives the value to write, the key to write it with, the
                dict-like object to write it into and the store_type being
                written

            TODO - these descriptions need updating
        """
        super(ColumnDesc, self).__init__(
                doc=doc, col_cls=col_cls, type=type, store_type=store_type)
        if key is None:
            self.key = None
        elif isinstance(key, Mapping):
            self.key = key
        else:
            self.key = {None: key}
        self.default = default
        self.read_func = read_func
        self.write_func = write_func

class Column(ColumnBase):
    """ Default column implementation
    
        A column is a property that returns all the values for the column as an
        iterator, but is not directly settable
    """
    def __init__(self, name, index, desc):
        def fget(obj):
            return AlgebraicGenerator(self.get(obj, row_idx) for row_idx in obj)
        ColumnBase.__init__(self, name=name, desc=desc, index=index, fget=fget)
        self._index = index

    def key(self, remote):
        """ The key for this column in the given remote source """
        if self._desc.key is None:
            return self.name
        try:
            return self._desc.key[remote]
        except KeyError as e:
            try:
                return self._desc.key[None]
            except KeyError:
                # Raise the original key error
                raise e

    def read_from(self, data, store_type):
        """ Read a key from an input dict with the given store type """
        return self._desc.read_func(
                self.key(store_type), data, self._desc.default, store_type)
            
    def write_to(self, value, data, store_type):
        """ Write a key to an output dict with the given store type """
        self._desc.write_func(value, self.key(store_type), data, store_type)

    @property
    def index(self):
        """ The index of this column """
        return self._index

    def get(self, db, row_idx):
        """ Get the value of this column in the specified row """
        return self.type(db._store[row_idx, self.index])

    def set(self, db, row_idx, value):
        """ Set the value of this column in the specified row
        
            This will only work if the underlying store permits modification
        """
        db._store[row_idx, self.index] = self.store_type(value)

class Field(property):
    def __init__(self, column):
        self._column = column
        def fget(obj):
            return self.column.get(obj.database, obj._index)
        def fset(obj, value):
            self.column.set(obj.database, obj._index, value)
        property.__init__(self, fget=fget, fset=fset)
        self.__doc__ = "Field for {0}".format(self.name)

    @property
    def column(self):
        """ The column in the database referred to by this field"""
        return self._column

    @property
    def name(self):
        """ The name of the field """
        return self.column.name

def index_reader(f):
    """ Decorator that makes a type conversion function into a valid index
        read_func

        If the provided function does not have a 'source' kwarg an ignored one
        will be added, otherwise the undecorated function is returned
    """
    sig = signature(f)
    if "source" in sig.parameters:
        return f
    @wraps(f)
    def wrapper(x, source):
        return f(x)
    return wrapper

index_read_identity = index_reader(identity)
 
def index_writer(f):
    """ Decorator that makes a type conversion function into a valid index
        write_func

        If the provided function does not have a 'target' kwarg an ignored one
        will be added, otherwise the undecorated function is returned
    """
    sig = signature(f)
    if "target" in sig.parameters:
        return f
    @wraps(f)
    def wrapper(x, target):
        return f(x)
    return wrapper

index_write_identity = index_writer(identity)

class IndexColumnDesc(ColumnDescBase):
    """ Describe the index column

        There should only be one index column description in a given database
        class (though adding one in a base class is allowed)
    """
    def __init__(
            self, doc=None, col_cls=None, type=identity, store_type=identity,
            read_func=index_read_identity, write_func=index_write_identity):
        """ Create the description

            Parameters:
                doc: The docstring for the column
                col_cls: The column class to be created from this description

            key should be a mapping from remote store type to the key of this
            column in that store, with None representing the default value. If a
            string is provided then key will be replaced internally by {None:
            key}. If no value is provided then key will be replaced internally
            with {None: name} where name is the attribute name of the column on
            the database class

            The doc string can be a format string, its format function will be
            called with keyword arguments name and index (the name and index of
            the column)

            Conversions
            -----------
            There are four conversion functions that can be supplied.
            type:
                Applied when reading a field directly from the database. This
                rarely needs to be anything other than identity but might be
                useful when overriding an existing column via inheritance
            store_type:
                Applied when writing a field directly back into the database,
                should be the inverse of type
            read_func:
                Applied when reading a field *into* the database from some
                remote store (e.g. a JSON file). In order to support multiple
                types of remote store, each remote store should be named to
                allow this function to behave differently for different type.
                This name is provided as the source keyword parameter.
            write_func:
                The inverse of read_func, applied when writing the field back
                into the remote store. For this function the remote store type
                is provided as the target keyword parameter. The function
                receives the value to write, the key to write it with, the
                dict-like object to write it into and the store_type being
                written
        """
        if col_cls is None:
            col_cls = IndexColumn
        super(IndexColumnDesc, self).__init__(
                doc=doc, col_cls=col_cls, type=type, store_type=store_type)
        self.read_func = read_func
        self.write_func = write_func

class IndexColumn(ColumnBase):
    """ The index column """
    def __init__(self, name, desc):
        def fget(obj):
            return AlgebraicGenerator(iter(obj))
        super(IndexColumn, self).__init__(name=name, desc=desc, fget=fget)

    @property
    def read_func(self):
        """ The conversion from remote store -> local store """
        return self._desc.read_func

    @property
    def write_func(self):
        """ The conversion from local store -> remote store """
        return self._desc.write_func

class IndexField(property):
    def __init__(self, column):
        self._column = column
        def fget(obj):
            return obj._index if column._desc.type is None \
                    else column._desc.type(obj._index)
        property.__init__(self, fget=fget)
        self.__doc__ = "Index field"

    @property
    def name(self):
        """ The name of the field """
        return self.column.name

    @property
    def column(self):
        """ The column in the database referred to by this field"""
        return self._column
