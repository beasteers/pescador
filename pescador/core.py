#!/usr/bin/env python
"""Core classes"""
try:
    import collections.abc as collections_abc  # only works on python 3.3+
except ImportError:
    import collections as collections_abc
import copy
import inspect
import six

from decorator import decorator

from .exceptions import PescadorError


class Streamer(object):
    '''A wrapper class for recycling iterables and generator functions, i.e.
    streamers.

    Wrapping streamers within an object provides
    two useful features:

    1. Streamer objects can be serialized (as long as its streamer can be)
    2. Streamer objects can instantiate a generator multiple times.

    The first feature is important for parallelization (see `zmq_stream`),
    while the second feature facilitates infinite streaming from finite data
    (i.e., oversampling).


    Attributes
    ----------
    streamer : generator or iterable
        Any generator function or iterable python object.

    args : list
    kwargs : dict
        Parameters provided to `streamer`, if callable.

    Examples
    --------
    Generate random 3-dimensional vectors

    >>> def my_generator(n):
    ...     for i in range(n):
    ...         yield i
    >>> stream = Streamer(my_generator, 5)
    >>> for i in stream:
    ...     print(i)  # Displays 0, 1, 2, 3, 4


    Or with a maximum number of items

    >>> for i in stream(max_items=3):
    ...     print(i)  # Displays 0, 1, 2


    Or infinitely many examples, restarting the generator as needed

    >>> for i in stream.cycle():
    ...     print(i)  # Displays 0, 1, 2, 3, 4, 0, 1, 2, ...

    Or finitely many examples, restarting the generator as needed

    >>> for i in stream.cycle(max_iter=7):
    ...     print(i)  # Displays 0, 1, 2, 3, 4, 0, 1


    An alternate interface for the same:

    >>> for i in stream(cycle=True):
    ...     print(i)  # Displays 0, 1, 2, 3, 4, 0, 1, 2, ...

    '''

    def __init__(self, streamer, *args, **kwargs):
        '''Initializer

        Parameters
        ----------
        streamer : iterable or generator function
            Any generator function or object that is iterable when
            instantiated.

        args, kwargs
            Additional positional arguments or keyword arguments passed to
            ``streamer()`` if it is callable.

        Raises
        ------
        PescadorError
            If ``streamer`` is not a generator or an Iterable object.

        '''

        if not (inspect.isgeneratorfunction(streamer) or
                isinstance(streamer, (collections_abc.Iterable, Streamer))):
            raise PescadorError('`streamer` must be an iterable or callable '
                                'function that returns an iterable object.')

        # The iterable or callable to stream from
        self.streamer = streamer

        # Args and kwargs are passed to an instantiated function
        self.args = args
        self.kwargs = kwargs

        # When a stream is activated, a copy of this streamer is made.
        # The number of copies is tracked with active_count_.
        self.active_count_ = 0

        # Stream points to the activated generator. This is only used
        # in the copy created.
        self.stream_ = None

    def __str__(self):
        # streamer get argument list
        args = ['{!r}'.format(x) for x in self.args]
        args.extend('{}={!r}'.format(k, v) for k, v in self.kwargs)
        args = '({})'.format(', '.join(args)) if args else ''

        # get streamer name
        return '<{} ({}{})>'.format(
            self.__class__.__name__,
            getattr(self.streamer, '__name__', self.streamer),
            args)

    def __copy__(self):
        cls = self.__class__
        copy_result = cls.__new__(cls)
        copy_result.__dict__.update(self.__dict__)
        return copy_result

    _NO_DEEPCOPY = []

    def _no_deepcopy(self, k, v):
        return k in self._NO_DEEPCOPY

    def __deepcopy__(self, memo):
        cls = self.__class__
        copy_result = cls.__new__(cls)
        memo[id(self)] = copy_result
        for k, v in six.iteritems(self.__dict__):
            if self._no_deepcopy(k, v):
                setattr(copy_result, k, v)
            else:
                setattr(copy_result, k, copy.deepcopy(v, memo))

        return copy_result

    def __enter__(self, *args, **kwargs):
        # If this is the base / original streamer,
        #  create a copy and return it
        if not self.is_activated_copy:
            streamer_copy = copy.deepcopy(self)
            streamer_copy._activate()

            # Increment the count of active streams.
            self.active_count_ += 1

        # However, if this is an "activated" streamer, then it is a copy,
        #  so just return self.
        else:
            streamer_copy = self

        return streamer_copy

    def __exit__(self, *exc):
        if not self.is_activated_copy:

            # Decrement the count of active streams.
            self.active_count_ -= 1

            if self.active_count_ < 0:
                raise PescadorError("Active stream count passed below 0 for {}"
                                    .format(self))
        self.close()
        return False

    def close(self):
        if self.is_activated_copy:
            if hasattr(self.stream_, 'close'):
                return self.stream_.close()

    @property
    def active(self):
        """Returns true if the stream is active
        (ie there are still open / existing streams)
        """
        return self.active_count_

    @property
    def is_activated_copy(self):
        """is_active is true if this object is a copy of the original Streamer
        *and* has been activated.
        """
        return self.stream_ is not None

    def _activate(self):
        """Activates the stream."""
        if six.callable(self.streamer):
            # If it's a function, create the stream.
            self.stream_ = self.streamer(*self.args, **self.kwargs)

        else:
            # If it's iterable, use it directly.
            self.stream_ = iter(self.streamer)

    def iterate(self, max_iter=None):
        '''Instantiate an iterator.

        Parameters
        ----------
        max_iter : None or int > 0
            Maximum number of iterations to yield.
            If ``None``, exhaust the stream.

        Yields
        ------
        obj : Objects yielded by the streamer provided on init.

        See Also
        --------
        cycle : force an infinite stream.

        '''
        # Use self as context manager / calls __enter__() => _activate()
        with self as active_streamer:
            with active_streamer:  # so exit always gets called on active streamer
                for n, obj in enumerate(active_streamer.stream_):
                    if max_iter is not None and n >= max_iter:
                        break
                    yield obj

    def cycle(self, max_iter=None):
        '''Iterate from the streamer infinitely.

        This function will force an infinite stream, restarting
        the streamer even if a StopIteration is raised.

        Parameters
        ----------
        max_iter : None or int > 0
            Maximum number of iterations to yield.
            If `None`, iterate indefinitely.

        Yields
        ------
        obj : Objects yielded by the streamer provided on init.
        '''

        count = 0
        while True:
            for obj in self.iterate():
                count += 1
                if max_iter is not None and count > max_iter:
                    return
                yield obj

    def __call__(self, max_iter=None, cycle=False):
        '''Convenience interface for interacting with the Streamer.

        Parameters
        ----------
        max_iter : None or int > 0
            Maximum number of iterations to yield.
            If `None`, attempt to exhaust the stream.
            For finite streams, call iterate again, or use `cycle=True` to
            force an infinite stream.

        cycle: bool
            If `True`, cycle indefinitely.

        Yields
        ------
        obj : Objects yielded by the generator provided on init.

        See Also
        --------
        iterate
        cycle
        '''
        if cycle:
            gen = self.cycle(max_iter=max_iter)
        else:
            gen = self.iterate(max_iter=max_iter)

        for obj in gen:
            yield obj

    def __iter__(self):
        return self.iterate()


@decorator
def streamable(function, *args, **kwargs):
    '''Create a Streamer object by decoration.

    This is a convenient shortcut for declaring generator
    functions as Streamable, rather than having to construct
    object wrappers explicitly every time.

    Examples
    --------
    This example constructs a generator and then wraps it
    in a Streamer object:

    >>> def myfunc(n):
    ...     for i in range(n):
    ...         yield i
    >>> s1 = Streamer(myfunc, 5)

    Equivalently, you can use the decorator to achieve the
    same effect:

    >>> @pescador.streamable
    ... def myfunc2(n):
    ...     for i in range(n):
    ...         yield i
    >>> s2 = myfunc2(5)
    '''

    return Streamer(function, *args, **kwargs)


class _WarmedUpStreamer(Streamer):
    '''Normally streamers aren't activated until they are first iterated over.

    This will activate each streamer as soon as iterate() is called.
    '''
    def _iterate_active(self, max_iter=None):
        '''Iterate over items in the queue.'''
        for n, obj in enumerate(self.stream_):  # like core.Streamer
            if max_iter is not None and n >= max_iter:
                break
            yield obj

    def _inner_iterate_wrapper(self, **kw):
        with self as active:
            with active:  # make sure __exit__ gets called on active
                yield # warmup
                yield from active._iterate_active(**kw)

    def iterate(self, max_iter=None):
        # Need to initialize the iterator first because otherwise they'll
        # only be activated when they are first iterated over
        gen = self._inner_iterate_wrapper(max_iter=max_iter)
        next(gen)  # warmup
        return gen
