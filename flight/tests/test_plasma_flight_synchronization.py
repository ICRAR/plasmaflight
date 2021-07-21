from dataclasses import make_dataclass
import subprocess as sp
import unittest
from six import BytesIO, StringIO

import asyncio
import os
import time
import hashlib
import pandas
import numpy as np
import pyarrow
import pyarrow.flight
import pyarrow.plasma as plasma

from server.plasmaflight_server import PlasmaFlightServer
from client.plasmaflight_client import PlasmaFlightClient, generate_sha1_object_id

class TestPlasmaFlightSynchronization(unittest.TestCase):
    """Tests replicating plasma store over a network"""
    def setUp(self):
        self._store0 = sp.Popen(["plasma_store", "-m", "100000000", "-s", "/tmp/plasma0"])
        self._server0 = PlasmaFlightServer(
            location="grpc+tcp://localhost:5005",
            plasma_socket="/tmp/plasma0",
            tls_certificates=[],
            verify_client=False)

        self._store1 = sp.Popen(["plasma_store", "-m", "100000000", "-s", "/tmp/plasma1"])
        self._server1 = PlasmaFlightServer(
            location="grpc+tcp://localhost:5006",
            plasma_socket="/tmp/plasma1",
            tls_certificates=[],
            verify_client=False)

        self._client0 = PlasmaFlightClient("/tmp/plasma0")
        self._client1 = PlasmaFlightClient("/tmp/plasma1")

    def tearDown(self):
        self._server0._shutdown()
        self._store0.terminate()
        self._server1._shutdown()
        self._store1.terminate()

    def test_string(self):
        message = "你好"
        input = message.encode('utf-8')
        buffer = memoryview(input)
        object_id = generate_sha1_object_id(input)
        # local client
        self._client0.put(buffer, object_id)
        output = self._client0.get(object_id).tobytes().decode('utf-8')
        assert output == message
        # remote client
        output = self._client1.get(object_id, "localhost:5005").tobytes().decode('utf-8')
        assert output == message
        # remote cache
        output = self._client1.get(object_id).tobytes().decode('utf-8')
        assert output == message
        # remote cache (even if owner is specified)
        self._server0._shutdown()
        output = self._client1.get(object_id, "localhost:5005").tobytes().decode('utf-8')
        assert output == message

    def test_tensor(self):
        tensor = np.array([[[1,2],[3,4]],[[5,6],[7,8]]])
        data = BytesIO()
        np.save(data, tensor)
        buffer: memoryview = data.getbuffer()
        object_id = generate_sha1_object_id(b'2x2x2')
        # local client
        self._client0.put(buffer, object_id)
        output = np.load(BytesIO(self._client0.get(object_id)))
        assert np.array_equal(output, tensor)
        # remote client
        output = np.load(BytesIO(self._client1.get(object_id, "localhost:5005")))
        assert np.array_equal(output, tensor)
        # remote cache
        output = np.load(BytesIO(self._client1.get(object_id)))
        assert np.array_equal(output, tensor)
        # remote cache (even if owner is specified)
        self._server0._shutdown()
        output = np.load(BytesIO(self._client1.get(object_id, "localhost:5005")))
        assert np.array_equal(output, tensor)

