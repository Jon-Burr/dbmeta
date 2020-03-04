from future.utils import PY3
from itertools import repeat
import operator
if PY3:
    from collections.abc import Iterator
else:
    from collections import Iterator

class ItrMonad(Iterator):
    """ Special type of iterator that allows forwarding attribute retrieval,
        function calls, etc to the iterated objects

        Operators can be forwarded
        >>> itr = ItrMonad(iter([0, 1, 2, 3, 4, 5]))
        >>> print(list(itr * 2 + 1))
        [1, 3, 5, 7, 9, 11]

        As can member function calls
        >>> itr = ItrMonad(iter(["Hello {0}", "Goodbye {0}"]))
        >>> print(list(itr.format("World")))
        ["Hello World", "Goodbye World"]

        If a provided argument is an iterator, it will be izipped together when
        called
        >>> itr1 = ItrMonad(iter([0, 1, 2, 3, 4, 5]))
        >>> itr2 = ItrMonad(iter([0, 2, 4, 6, 8]))
        >>> print(list(itr1 + itr2))
        [0, 3, 6, 9, 12]

        Note here that the normal izip behaviour of ending when the shortest iterator 

        A non member function can also be called using apply
        >>> itr1 = iter([0, 1, 2, 3, 4, 5])
        >>> print(list(ItrMonad.apply(str.format, "x = {0}", itr1)))
        ['x = 0', 'x = 1', 'x = 2', 'x = 3', 'x = 4', 'x = 5']
    """
    def __init__(self, itr):
        self._itr = itr

    @classmethod
    def apply(cls, func, *args, **kwargs):
        """ Apply a function elementwise for iterables
        
            Any argument that is not an Iterator will be replaced by
            itertools.repeat. This means that if no arguments are iterators, the
            returned iterator will never be exhausted!
        """
        def to_repeat(x):
            if isinstance(x, Iterator):
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

    def __iter__(self):
        return self

    if PY3:
        def __next__(self):
            return next(self._itr)
    else:
        def next(self):
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
