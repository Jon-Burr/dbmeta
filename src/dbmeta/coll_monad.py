from builtins import zip
from future.utils import PY3, iteritems
from itertools import repeat
import operator
if PY3:
    from collections.abc import Iterator, Iterable
else:
    from collections import Iterator, Iterable

class CollMonad(Iterable):
    """ Special type of iterable that allows forwarding attribute retrieval,
        function calls, etc to the iterated objects.
        
        The examples here show ItrMonad but TupleMonad works in the same way

        Operators can be forwarded
        >>> itr = ItrMonad(iter([0, 1, 2, 3, 4, 5]))
        >>> list(itr * 2 + 1)
        [1, 3, 5, 7, 9, 11]

        As can member function calls
        >>> itr = ItrMonad(iter(["Hello {0}", "Goodbye {0}"]))
        >>> list(itr.format("World"))
        ["Hello World", "Goodbye World"]

        If a provided argument is an iterator, it will be izipped together when
        called
        >>> itr1 = ItrMonad(iter([0, 1, 2, 3, 4, 5]))
        >>> itr2 = ItrMonad(iter([0, 2, 4, 6, 8]))
        >>> list(itr1 + itr2)
        [0, 3, 6, 9, 12]

        Note here that the normal izip behaviour of ending when the shortest iterator 

        A non member function can also be called using apply
        >>> itr1 = iter([0, 1, 2, 3, 4, 5])
        >>> list(ItrMonad.apply(str.format, "x = {0}", itr1))
        ['x = 0', 'x = 1', 'x = 2', 'x = 3', 'x = 4', 'x = 5']

        The class also provides special methods for doing element-wise boolean
        operations with the expected short-circuiting behaviour. This example
        uses TupleMonad to avoid having to redeclare the initial tuple but the
        behaviour is the same for ItrMonad.

        >>> tup1 = TupleMonad([0, 1, 2, 3, 4, 5])
        >>> TupleMonad.and_(tup1 > 2, tup1 < 4)
        TupleMonad(False, False, False, True, False, False)
        >>> TupleMonad.or_(tup1 < 2, tup1 > 4)
        TupleMonad(True, True, False, False, False, True)
        >>> TupleMonad.all(tup1 > 1, tup1 < 4, tup1 % 2 == 0)
        TupleMonad(False, False, True, False, False, False)
        >>> TupleMonad.any(tup1 < 1, tup1 > 4, tup1 % 3 == 0)
        TupleMonad(True, False, False, True, False, True)

        There is also a special in_ method for checking membership
        >>> tup1 = TupleMonad([0, 1, 2, 3, 4, 5])
        >>> TupleMonad.in_(tup1, (0, 3, 4))
        TupleMonad(True, False, False, True, True, False)
    """
    @classmethod
    def apply(cls, func, *args, **kwargs):
        """ Apply a function elementwise for iterables
        
            Any argument that is not an Iterator will be replaced by
            itertools.repeat. This means that if no arguments are iterators, the
            returned iterator will never be exhausted!

            Note that this uses Iterators, rather than Iterables to avoid
            zipping objects like strings.
        """
        def to_repeat(x):
            # Note the use of CollMonad here rather than cls - this is to avoid
            # derived classes not counting each other properly
            if isinstance(x, (CollMonad, Iterator)):
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
            g_kw = (dict(zip(kwargs.keys(), vs)) for vs in zip(*kwargs.values() ) )
        else:
            g_kw = repeat({})
        args.append(g_kw)
        return cls(func(*a[:-1], **a[-1]) for a in zip(*args))

    @classmethod
    def in_(cls, lhs, rhs):
        """ Elementwise 'lhs in rhs' """
        return cls.apply(lambda x, y: x in y, lhs, rhs)

    @classmethod
    def and_(cls, lhs, rhs):
        """ Elementwise 'and' of lhs and rhs """
        return cls.apply(lambda x, y: x and y, lhs, rhs)

    @classmethod
    def or_(cls, lhs, rhs):
        """ Elementwise 'or' of lhs and rhs """
        return cls.apply(lambda x, y: x or y, lhs, rhs)

    @classmethod
    def any(cls, *args):
        """ Apply the any function elementwise """
        return cls.apply(lambda *args: any(args), *args)

    @classmethod
    def all(cls, *args):
        """ Apply the all function elementwise """
        return cls.apply(lambda *args: all(args), *args)

    @classmethod
    def flatten(cls, iterable, cls_tup=None, no_expand=None):
        """ Flatten an arbitarily (modulo stack limit) nested iterable
        
            By default this only expands CollMonads, Iterators, lists and tuples
            (to avoid unfolding strings) but this can be modified by passing a
            tuple of types to expand to cls_tup. Types can be excluded from the
            expansion by passing a tuple to no_expand
        """
        if cls_tup is None:
            cls_tup = (CollMonad, Iterator, list, tuple)
        if no_expand is None:
            no_expand = ()
        def iter_flatten(itrbl):
            itr = iter(itrbl)
            for ele in itr:
                if isinstance(ele, cls_tup) and not isinstance(ele, no_expand):
                    for ele2 in iter_flatten(ele):
                        yield ele2
                else:
                    yield ele
        return cls(iter_flatten(iterable))


    def call(self, func, *args, **kwargs):
        """ Call the given function for each member of the iterable with that
            member as the first argument

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

class ItrMonad(CollMonad, Iterator):
    """ CollMonad that acts as an iterator

        Only valid for a single pass, but likely to be more efficient for most
        operations (each individual calculation can short circuit, for example)

        If the iterator has side effects, it can be run through by calling
        invoke. After this the iterator will be exhausted.
    """
    def __init__(self, itr):
        if not isinstance(itr, Iterator):
            itr = iter(itr)
        self._itr = itr

    def __iter__(self):
        return self

    def invoke(self):
        """ Evaluate the iterator, causing any side effects to occur """
        for _ in self:
            pass

    if PY3:
        def __next__(self):
            return next(self._itr)
    else:
        def next(self):
            return next(self._itr)

class TupleMonad(CollMonad):
    """ CollMonad that acts as a tuple

        The whole result of the calculation is stored and can be iterated
        through multiple times.

        Has a helper 'select' function that returns a filtered result
        >>> tup = TupleMonad([0, 1, 2, 3, 4, 5])
        >>> tup.select(tup % 2 == 0)
        TupleMonad(0, 2, 4)
    """

    def __init__(self, itr):
        self._tup = tuple(itr)

    def __iter__(self):
        return ItrMonad(self._tup)

    def __str__(self):
        return str(self._tup)

    def __repr__(self):
        return "TupleMonad{0}".format(self._tup)

    def __len__(self):
        return len(self._tup)

    def __contains__(self, x):
        return x in self._tup

    def select(self, selection):
        return TupleMonad(x for (x, sel) in zip(self, selection) if sel)
