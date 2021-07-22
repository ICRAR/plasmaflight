#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
from dataclasses import make_dataclass
import subprocess as sp
import unittest
from io import BytesIO

import pandas
import numpy as np
import pyarrow
import pyarrow.plasma as plasma

from plasmaflight.server.plasmaflight_server import PlasmaUtils

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

    def test_ndarray_raw(self):
        a = np.array([[1,2],[3,4]])
        assert(a.data.tobytes() == a.tobytes())
        abytes = a.tobytes()

        b = np.frombuffer(abytes, dtype=a.dtype).reshape(2,2)
        assert(np.array_equal(a, b))
    
    def test_ndarray_bytesio(self):
        a = np.array([[1,2],[3,4]])
        np_bytes = BytesIO()
        np.save(np_bytes, a, allow_pickle=True)
        abytes = np_bytes.getvalue()

        load_bytes = BytesIO(abytes)
        b = np.load(load_bytes, allow_pickle=True)
        assert(np.array_equal(a,b))

