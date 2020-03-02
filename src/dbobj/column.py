from __future__ import print_function
from future.utils import iteritems
from builtins import object, zip
from itertools import repeat
from inspect import isgenerator
import operator

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
        args = (self,) + args
        return type(self).apply(func, *args, **kwargs)

    def __iter__(self):
        return self._itr

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

class ColumnDesc(object):
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
            self, doc=None, key=None, type=None, col_cls=None, store_type=None,
            default=NO_DEFAULT, is_index=False):
        """ Create the description

            Parameters:
                doc: The docstring for the column
                key: The name of the column in the store
                type: The type to be stored in the column
                col_cls: The column class to be created from this description
                store_type: The type of the column in the internal store
                default: The default value this column should take
                is_index: If this column is the index in an associative DB

            If key is not set, it will be set from the attribute name in the
            database class

            If type is set, it should be a class to convert the stored values
            on return (it can also be a function - the important thing is that
            it is a mapping from the stored value to what is output

            store_type should then be the inverse of the type function. It is
            only needed if the store will allow setting information (i.e. is not
            readonly)

            The doc string can be a format string, its format function will be
            called with keyword arguments name and index (the name and index of
            the column)
        """
        if is_index:
            if default is not ColumnDesc.NO_DEFAULT:
                raise ValueError("Cannot specify a default for the index column")
            if key is not None:
                raise ValueError("Cannot specify a key for the index column")
        if doc is None:
            doc = "The {name} column in the database"
        self.doc = doc
        self.key = key
        self.type = type
        self.col_cls = col_cls
        self.store_type = store_type
        self.default = default
        self.is_index = is_index

class Column(property):
    """ Default column implementation
    
        A column is a property that returns all the values for the column as an
        iterator, but is not directly settable
    """
    def __init__(self, name, index, desc):
        def fget(obj):
            return AlgebraicGenerator(self.get(obj, row_idx) for row_idx in obj)
        property.__init__(self, fget=fget)
        self.__doc__ = desc.doc.format(name=name, index=index)

        self._name = name
        self._index = index
        self._desc = desc

    @property
    def name(self):
        """ The name of this column 
            
            The name should be the name of the attribute that this is access
            through on the database/row classes
        """
        return self._name

    @property
    def key(self):
        """ The key for this column

            The key is the name used in the store
        """
        return self._desc.key if self._desc.key is not None else self.name

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

    def get(self, db, row_idx):
        """ Get the value of this column in the specified row """
        stored_val = db._store[row_idx, self.index]
        return stored_val if self.type is None else self.type(stored_val)

    def set(self, db, row_idx, value):
        """ Set the value of this column in the specified row
        
            This will only work if the underlying store permits modification
        """
        # Convert if necessary
        if self.store_type is not None:
            value = self.store_type(value)
        db._store[row_idx, self.index] = value

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

class IndexColumn(property):
    """ The index column """
    def __init__(self, name, desc):
        def fget(obj):
            return AlgebraicGenerator(obj)
        property.__init__(self, fget=fget)
        self.__doc__ = desc.doc.format(name=name)
        self._name = name
        self._desc = desc

    @property
    def name(self):
        """ The name of this column 
            
            The name should be the name of the attribute that this is access
            through on the database/row classes
        """
        return self._name

    @property
    def type(self):
        """ The conversion from stored -> returned when getting """
        return self._desc.type

    @property
    def store_type(self):
        """ The conversion from set value -> stored when setting """
        return self._desc.store_type

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
