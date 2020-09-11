import pytest
import numpy as np
import six
import pescador
import test_utils as T


@pytest.mark.parametrize('maxlen', [None, 1, 10, 100, 500])
def test_preload(maxlen):
    stream = pescador.Streamer(T.finite_generator, 200, size=3, lag=0.001)
    reference = list(stream)

    preload = pescador.PreloadStreamer(stream, maxlen=maxlen)

    for _ in range(3):
        query = list(preload)
        assert len(reference) == len(query)
        for b1, b2 in zip(reference, query):
            T._eq_batch(b1, b2)


def test_zmq_align():

    stream = pescador.Streamer(T.finite_generator, 200, size=3, lag=0.001)
    reference = list(stream)

    preload = pescador.PreloadStreamer(stream)
    query = list(preload)

    assert len(reference) == len(query)

    for b1, b2 in zip(reference, query):
        T._eq_batch(b1, b2)
        for key in b2:
            assert b2[key].flags['ALIGNED']


def test_zmq_early_stop():
    stream = pescador.Streamer(T.finite_generator, 200, size=3, lag=0.001)

    preload = pescador.PreloadStreamer(stream)

    # Only sample five batches
    assert len([x for x in preload.iterate(5)]) == 5
    assert not preload._running


def test_zmq_buffer():
    n_samples = 50
    stream = pescador.Streamer(T.md_generator, dimension=2, n=n_samples,
                               size=64, items=['X', 'Y'])

    buff_size = 10
    buff_stream = pescador.Streamer(pescador.buffer_stream, stream, buff_size)
    preload = pescador.PreloadStreamer(buff_stream)

    outputs = [x for x in preload]
    assert len(outputs) == int(n_samples) / buff_size
