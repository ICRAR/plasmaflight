
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

from server.plasmaflight_server import PlasmaUtils, PlasmaFlightServer
from client.plasmaflight_client import PlasmaFlightClient, list_flights

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

        self._client = PlasmaFlightClient(host, port, scheme)

    def tearDown(self):
        self._server._shutdown()
        self._store.terminate()

    def test_list(self):
        flights = list(self._client.list_flights())
        assert len(flights) == 0

    def test_push_pull_string(self):
        message = "Hello World!"
        input = message.encode('utf-8')
        buffer = memoryview(input)
        object_id = generate_sha1_object_id(input)
        self._client.put(buffer, object_id)
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

    def test_push_pull_tensor(self):
        tensor = np.array([[[1,2],[3,4]],[[5,6],[7,8]]])
        data = BytesIO()
        np.save(data, tensor)
        buffer: memoryview = data.getbuffer()
        object_id = generate_sha1_object_id('2x2x2'.encode('utf-8'))
        self._client.put(buffer, object_id)
        output = np.load(BytesIO(self._client.get(object_id)))
        assert np.array_equal(output, tensor)

    # def test_push_pull_dataframe(self):
    #     data = {
    #         'a': ['this', 'is', 'a', 'test'],
    #         'b': [1,2,3,4]
    #     }
    #     df = pandas.DataFrame(data)
    #     table = pyarrow.Table.from_pandas(df)
    #     object_id = generate_sha1_object_id('test_table'.encode('utf-8'))
    #     self._client.put_table(table, object_id)
        
    #     for flight in self._client.list_flights():
    #         print(flight.schema)
    #         print(flight.total_records)
    #         print(flight.total_bytes)
    #         print(flight.endpoints)

    #     #output = self._client.get_table(object_id).to_pandas()
    #     #print(output)
    #     assert False

    # def test_pull_string(self):
    #     pass

    # def test_pull_tensor(self):
    #     pass

    # def test_pull_dataframe(self):
    #     pass
    

class TestPlasmaFlightService(unittest.TestCase):
    """Tests replicating plasma store over a network"""
    def setUp(self):
        pass

    def tearDown(self):
        pass

