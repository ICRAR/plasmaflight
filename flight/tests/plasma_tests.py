
from dataclasses import make_dataclass
import subprocess as sp
import unittest

import pandas

import pyarrow
import pyarrow.plasma as plasma

from server.plasmaflight_server import PlasmaUtils

class TestPlasma(unittest.TestCase):
    PLASMA_SOCKET = '/tmp/plasma'

    def setUp(self):
        self._store = sp.Popen(["plasma_store", "-m", "100000000", "-s", "/tmp/plasma"])

    def tearDown(self):
        self._store.terminate()

    def test_string(self):
        self.client = plasma.connect(self.PLASMA_SOCKET)

        data = "Hello World"
        object_id = plasma.ObjectID.from_random()
        self.client.put(data, object_id)
        output = self.client.get(object_id)

        assert(output == data)

    def test_pandas_dataframe(self):
        self.client = plasma.connect(self.PLASMA_SOCKET)

        Point = make_dataclass("Point", [("name", str),("x", int),("y", int)])
        data = pandas.DataFrame([
            Point("start", 0, 0),
            Point("mid", 2, 1),
            Point("end", 5, 3)
        ])
        object_id = plasma.ObjectID.from_random()
        PlasmaUtils.put_dataframe(self.client, data, object_id)
        output = PlasmaUtils.get_dataframe(self.client, object_id)

        assert(output.equals(data))

    def test_pyarrow_table(self):
        pass
