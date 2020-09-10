import time
import queue
import threading
from .core import Streamer, _WarmedUpStreamer


class PreloadStreamer(_WarmedUpStreamer):
    _delay = 1e-3
    _max_iter = None
    _exception = None
    thread = q = None
    _running = False
    _NO_DEEPCOPY = ('thread', 'q')

    def __init__(self, streamer, maxlen=10):
        super().__init__(streamer)
        self.maxlen = maxlen

    def _activate(self):
        # start thread and queue if not already
        if self.thread is None:
            self._running = True
            self.q = queue.Queue(self.maxlen or 0)
            self.thread = threading.Thread(
                target=self._iterate_stream_worker,
                name=str(self), daemon=True)
            self.thread.start()
        super()._activate()

    def _iterate_stream_worker(self):
        '''thread worker that pulls items from the iterator and adds them to a queue.'''
        try:
            for i, x in enumerate(Streamer.iterate(self, self._max_iter)):
                # custom blocking
                while self.q.full() and self._running:
                    time.sleep(self._delay)
                if not self._running:
                    return

                self.q.put(x, block=False)
                if not self._running:
                    return
        except BaseException as e:
            self._exception = e

    def _iterate_active(self):
        '''Iterate over items in the queue.'''
        while True:
            # custom blocking
            while self._running and self.q.empty():
                time.sleep(self._delay)
            if not self._running and self.q.empty():
                return

            x = self.q.get(block=False)
            if x is not None: # generate value
                yield x
            if self._exception: # raise in main thread
                raise self._exception

    def iterate(self, max_iter=None):
        self._max_iter = max_iter  # need to get this to remote worker somehow
        return super().iterate()

    def close(self):
        super().close()
        # close thread (can't close the current thread while inside)
        self._running = False
        if self.thread is not None and self.thread is not threading.current_thread():
            self.thread.join()
        self.thread = None
