#!/usr/bin/env python
'''
Parallel muxing
---------------

Streaming from a background process pool implemented with the ZeroMQ library.
The `ZMQMux` object wraps ordinary muxes for background execution.

.. autosummary::
    :toctree: generated/

    ZMQMux

'''
import six
import copy as copy_
import warnings
import concurrent.futures as cf
import multiprocessing as mp
import zmq

from .core import Streamer, _WarmedUpStreamer
from .zmq_stream import zmq_recv_data, zmq_worker, SafeFunction


__all__ = ['ZMQMux']


POOLS = {
    'process': cf.ProcessPoolExecutor,
    'thread': cf.ThreadPoolExecutor,
}


class ZMQMux(_WarmedUpStreamer):
    """Parallel data muxing over zeromq sockets using a process or thread pool.

    This allows active streamers to run in separate processes/threads
    from the consumer.

    A typical usage pattern is to construct a `Mux` object from a list of
    streamers and then use `ZMQMux` to execute the stream in one
    process while the other process consumes data.


    Examples
    --------
    >>> # Construct a mux object
    >>> mux = pescador.RoundRobinMux([
    ...     pescador.Streamer(my_generator, x) for x in range(5)
    ... ])
    >>> # Wrap the mux in a ZMQ mux
    >>> zmux = pescador.ZMQMux(mux)
    >>> # Process as normal
    >>> for data in zmux:
    ...     MY_FUNCTION(data)
    """
    # don't copy certain items
    _NO_DEEPCOPY = ('pool',)

    def __init__(self, mux, min_port=49152, max_port=65535, max_tries=100,
                 copy=False, timeout=5, mode='process', n_jobs=None):
        '''
        Parameters
        ----------
        mux : `pescador.Mux`
            The mux object

        min_port : int > 0
        max_port : int > min_port
            The range of TCP ports to use

        max_tries : int > 0
            The maximum number of connection attempts to make

        copy : bool
            Set `True` to enable data copying

        timeout : [optional] number > 0
            Maximum time (in seconds) to wait before killing subprocesses.
            If `None`, then the streamer will wait indefinitely for
            subprocesses to terminate.

        mode : str
            The pool type - can be `'thread'` or `'process'`. Default is
            `'process'`.

        n_jobs : int > 0
            The maximum number of pool workers. If
        '''
        # get the number of jobs to use and make sure it's greater than
        # the number of active streamers (if applicable).
        n_active = getattr(mux, 'n_active', None)
        n_jobs = n_jobs or n_active
        if n_jobs and n_active and n_jobs < n_active:
            RuntimeError(
                'The number of zmq_mux workers ({}) must be greater than the '
                'number of active streams ({}).'.format(n_jobs, n_active))

        # create the pool
        if mode not in POOLS:
            raise ValueError('Invalid Pool type {!r}. Must be in {}'.format(
                mode, set(POOLS)))

        # create the pool and restrict the number of jobs to be no more
        # than the total number of streamers.
        self.n_jobs = min(n_jobs or len(mux.streamers), len(mux.streamers))
        self.pool = POOLS[mode](self.n_jobs)

        # wrap the mux streamers in ZMQ pool workers
        # copy so that it doesn't modify the original object.
        mux = copy_.copy(mux)
        mux.streamers = [
            ZMQMuxStreamer(
                s, self.pool, min_port=min_port, max_port=max_port,
                max_tries=max_tries, copy=copy, timeout=timeout)
            for s in mux.streamers
        ]
        super().__init__(mux)

        # only works for threads - not available with process pool
        self.pool._thread_name_prefix = str(self)

    def __enter__(self):
        active = super().__enter__()
        restart_pool(active.pool).__enter__()
        return active

    def __exit__(self, *exc):
        suppressed = (super().__exit__(*exc), self.pool.__exit__(*exc))
        return any(suppressed)
    def _deactivate(self):
        super()._deactivate()
        self.pool.shutdown(wait=True)
        self.pool = None


class ZMQMuxStreamer(_WarmedUpStreamer):
    _terminate = _future = None
    _NO_DEEPCOPY = ('pool', '_manager', '_terminate', '_future')

    # needed bc: RuntimeError: Condition objects should only be shared between processes through inheritance
    _manager = mp.Manager()
    def __init__(self, streamer, pool=None,
                 min_port=49152, max_port=65535,
                 max_tries=100, copy=False, timeout=5, **kw):
        self.streamer = streamer
        self.pool = pool
        self.min_port = min_port
        self.max_port = max_port
        self.max_tries = max_tries
        self.copy = copy
        self.timeout = timeout
        super().__init__(streamer, **kw)

    def _activate(self):
        if six.PY2:
            warnings.warn('zmq_stream cannot preserve numpy array alignment '
                          'in Python 2', RuntimeWarning)
        # open socket
        context = zmq.Context()
        socket = context.socket(zmq.PAIR)
        port = socket.bind_to_random_port(
            'tcp://*', self.min_port, self.max_port, self.max_tries)

        try:
            # create terminate flag and submit job
            self._terminate = self._manager.Event()
            self._future = self.pool.submit(
                SafeFunction(zmq_worker),
                port, self.streamer, self._terminate,
                copy=self.copy)
            # when done, raise exception if any.
            self._future.add_done_callback(lambda fut: fut.result())

            # create streamer
            self.stream_ = iter_zmq_stream(socket)
        except RuntimeError:
            # the pool has been shutdown and someone tried to activate a stream.
            # set the stream to be an empty iterable.
            context.destroy()  # don't need the socket, clean up.
            self.stream_ = iter(())

    def _deactivate(self):
        '''Send terminate to worker.'''
        if self._terminate is not None:
            try:  # notify zmq worker to exit
                self._terminate.set()
            except FileNotFoundError:  # ignore if manager is already closed.
                pass
        if self._future is not None:
            try:  # wait for result
                self._future.result(timeout=self.timeout)
            except cf.TimeoutError:  # if it didn't exit in time, just cancel.
                self._future.cancel()
        # cleanup
        self._terminate = self._future = None


def restart_pool(pool):
    # reset the pool instance - can't create a new instance because
    # then the child streamers might have an outdated reference at
    # activation.
    # XXX: is there a better way to reuse a pool executor ??
    pool.shutdown()
    n_jobs = pool._max_workers
    pool.__dict__.clear()
    pool.__init__(n_jobs)
    return pool
        super()._deactivate()


def iter_zmq_stream(socket):
    '''Stream data from socket and clean up at the end.'''
    try:
        while True:
            yield zmq_recv_data(socket)
    except StopIteration:
        pass
    finally:
        socket.context.destroy()
