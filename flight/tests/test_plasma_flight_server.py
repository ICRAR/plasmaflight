
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
from client.plasmaflight_client import PlasmaFlightClient

def generate_sha1_object_id(path: bytes) -> plasma.ObjectID:
    m = hashlib.sha1()
    m.update(path)
    id = m.digest()[0:20]
    return plasma.ObjectID(id)

class TestPlasmaFlightClientServer(unittest.TestCase):
    """Tests the plasmaflight client server"""
    def setUp(self):
        plasma_socket = "/tmp/plasma"
        self._store = sp.Popen(["plasma_store", "-m", "100000000", "-s", plasma_socket])
        
        scheme = "grpc+tcp"
        host = "localhost"
        port = 5005
        location = "{}://{}:{}".format(scheme, host, port)
        tls_certificates = []
        self._server = PlasmaFlightServer(
            location=location,
            plasma_socket=plasma_socket,
            tls_certificates=tls_certificates,
            verify_client=False)

        self._client = PlasmaFlightClient("/tmp/plasma", scheme="grpc+tcp")

    def tearDown(self):
        self._server._shutdown()
        self._store.terminate()

    def reinit_clientserver(self):
        self._server._shutdown()
        self._server = PlasmaFlightServer(
            location="grpc+tcp://localhost:5005",
            plasma_socket="/tmp/plasma",
            tls_certificates=[],
            verify_client=False)
        self._client = PlasmaFlightClient("/tmp/plasma", scheme="grpc+tcp")

    def test_list(self):
        flights = list(self._client.list_flights("localhost:5005"))
        assert len(flights) == 0

    def test_push_pull_string(self):
        message = "Hello World!"
        input = message.encode('utf-8')
        buffer = memoryview(input)
        object_id = generate_sha1_object_id(input)
        self._client.put(buffer, object_id)
        output = self._client.get(object_id)
        assert output.tobytes().decode('utf-8') == message

    def test_reinit_string(self):
        message = "Hello World!"
        input = message.encode('utf-8')
        buffer = memoryview(input)
        object_id = generate_sha1_object_id(input)
        self._client.put(buffer, object_id)
        self.reinit_clientserver()
        output = self._client.get(object_id)
        assert output.tobytes().decode('utf-8') == message

    def test_push_pull_string_international(self):
        message = "你好"
        input = message.encode('utf-8')
        buffer = memoryview(input)
        object_id = generate_sha1_object_id(input)
        self._client.put(buffer, object_id)
        output = self._client.get(object_id)
        assert output.tobytes().decode('utf-8') == message

    def test_reinit_string_international(self):
        message = "你好"
        input = message.encode('utf-8')
        buffer = memoryview(input)
        object_id = generate_sha1_object_id(input)
        self.reinit_clientserver()
        self._client.put(buffer, object_id)
        output = self._client.get(object_id)
        assert output.tobytes().decode('utf-8') == message

    def test_push_pull_tensor(self):
        tensor = np.array([[[1,2],[3,4]],[[5,6],[7,8]]])
        data = BytesIO()
        np.save(data, tensor)
        buffer: memoryview = data.getbuffer()
        object_id = generate_sha1_object_id(b'2x2x2')
        self._client.put(buffer, object_id)
        output = np.load(BytesIO(self._client.get(object_id)))
        assert np.array_equal(output, tensor)
        
    def test_reinit_tensor(self):
        tensor = np.array([[[1,2],[3,4]],[[5,6],[7,8]]])
        data = BytesIO()
        np.save(data, tensor)
        buffer: memoryview = data.getbuffer()
        object_id = generate_sha1_object_id(b'2x2x2')
        self.reinit_clientserver()
        self._client.put(buffer, object_id)
        output = np.load(BytesIO(self._client.get(object_id)))
        assert np.array_equal(output, tensor)

