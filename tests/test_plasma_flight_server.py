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
import unittest
import subprocess as sp

from six import BytesIO, StringIO
import numpy as np
import pyarrow
import pyarrow.flight
import pyarrow.plasma as plasma

from icrar.plasmaflight import PlasmaFlightServer
from icrar.plasmaflight import PlasmaFlightClient, generate_sha1_object_id

class TestPlasmaFlightClientServer(unittest.TestCase):
    """Tests the plasmaflight client server"""
    def setUp(self):
        plasma_socket = "/tmp/plasma"
        self._store = sp.Popen(["plasma_store", "-m", "100000000", "-s", plasma_socket])
        scheme = "grpc+tcp"
        host = "localhost"
        port = 5005
        location = f"{scheme}://{host}:{port}"
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
        assert self._client.exists(object_id) == False
        self._client.put(buffer, object_id)
        assert self._client.exists(object_id) == True
        self.reinit_clientserver()
        assert self._client.exists(object_id) == True
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
        assert self._client.exists(object_id) == False
        self._client.put(buffer, object_id)
        assert self._client.exists(object_id) == True
        self.reinit_clientserver()
        assert self._client.exists(object_id) == True
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
        assert self._client.exists(object_id) == False
        self._client.put(buffer, object_id)
        assert self._client.exists(object_id) == True
        self.reinit_clientserver()
        assert self._client.exists(object_id) == True
        output = np.load(BytesIO(self._client.get(object_id)))
        assert np.array_equal(output, tensor)

