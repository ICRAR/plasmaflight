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
import pyarrow.flight
import pyarrow.plasma as plasma

from icrar.plasmaflight.server.plasmaflight_server import PlasmaFlightServer
from icrar.plasmaflight.client.plasmaflight_client import PlasmaFlightClient, generate_sha1_object_id

class TestPlasmaFlightSecurity(unittest.TestCase):
    """Tests replicating plasma store over a network"""
    def setUp(self):
        self._store0 = sp.Popen(["plasma_store", "-m", "100000000", "-s", "/tmp/plasma0"])
        self._store1 = sp.Popen(["plasma_store", "-m", "100000000", "-s", "/tmp/plasma1"])

    def tearDown(self):
        self._server._shutdown()
        self._store0.terminate()
        self._store1.terminate()

    def test_tls_no_client_validation(self):
        scheme = "grpc+tls"
        host = "localhost"
        port = 5000
        location = f"{scheme}://{host}:{port}"
        
        cert_path = "tests/FlightServerCert.crt"
        key_path = "tests/FlightServerKey.key"
        with open(cert_path, "rb") as cert_file:
            tls_cert_chain = cert_file.read()
        with open(key_path, "rb") as key_file:
            tls_private_key = key_file.read()

        tls_certificates = []
        tls_certificates.append((tls_cert_chain, tls_private_key))

        self._server = PlasmaFlightServer(
            location=location,
            plasma_socket="/tmp/plasma0",
            tls_certificates=tls_certificates,
            verify_client=False)
        
        self._client0 = PlasmaFlightClient("/tmp/plasma0", scheme=scheme)
        message = "Hello World!"
        input = message.encode('utf-8')
        buffer = memoryview(input)
        object_id = generate_sha1_object_id(input)
        self._client0.put(buffer, object_id)
        output0 = self._client0.get(object_id)
        assert output0.tobytes().decode('utf-8') == message

        def query():
            self._client1.get(object_id, "localhost:5000")

        connection_args = {}
        self._client1 = PlasmaFlightClient("/tmp/plasma1", scheme=scheme, connection_args=connection_args)
        self.assertRaises(pyarrow.flight.FlightUnavailableError, query)

        connection_args["tls_root_certs"] = tls_cert_chain
        self._client1 = PlasmaFlightClient("/tmp/plasma1", scheme=scheme, connection_args=connection_args)
        output1 = self._client1.get(object_id, "localhost:5000")
        assert output1.tobytes().decode('utf-8') == message
    
    def test_tls_with_client_validation(self):
        scheme = "grpc+tls"
        host = "localhost"
        port = 5000
        location = f"{scheme}://{host}:{port}"
        
        server_cert_path = "tests/FlightServerCert.crt"
        server_key_path = "tests/FlightServerKey.key"
        client_cert_path = "tests/FlightClientCert.crt"
        client_key_path = "tests/FlightClientKey.key"

        with open(server_cert_path, "rb") as cert_file:
            server_cert_chain = cert_file.read()
        with open(server_key_path, "rb") as key_file:
            server_private_key = key_file.read()
        with open(client_cert_path, "rb") as cert_file:
            client_cert_chain = cert_file.read()
        with open(client_key_path, "rb") as key_file:
            client_private_key = key_file.read()

        tls_certificates = []
        tls_certificates.append((server_cert_chain, server_private_key))

        self._server = PlasmaFlightServer(
            location=location,
            plasma_socket="/tmp/plasma0",
            tls_certificates=tls_certificates,
            root_certificates=client_cert_chain,
            verify_client=True)
        
        # querying local store never goes through the network
        self._client0 = PlasmaFlightClient("/tmp/plasma0", scheme=scheme)
        message = "Hello World!"
        input = message.encode('utf-8')
        buffer = memoryview(input)
        object_id = generate_sha1_object_id(input)
        self._client0.put(buffer, object_id)
        output0 = self._client0.get(object_id)
        assert output0.tobytes().decode('utf-8') == message


        def query():
            self._client1.get(object_id, "localhost:5000")

        connection_args = {}
        self._client1 = PlasmaFlightClient("/tmp/plasma1", scheme=scheme, connection_args=connection_args)
        self.assertRaises(pyarrow.flight.FlightUnavailableError, query)

        connection_args["tls_root_certs"] = server_cert_chain
        self._client1 = PlasmaFlightClient("/tmp/plasma1", scheme=scheme, connection_args=connection_args)
        self.assertRaises(pyarrow.flight.FlightUnavailableError, query)

        connection_args["cert_chain"] = client_cert_chain
        self._client1 = PlasmaFlightClient("/tmp/plasma1", scheme=scheme, connection_args=connection_args)
        self.assertRaises(pyarrow.flight.FlightUnavailableError, query)

        connection_args["private_key"] = client_private_key
        self._client1 = PlasmaFlightClient("/tmp/plasma1", scheme=scheme, connection_args=connection_args)
        output1 = self._client1.get(object_id, "localhost:5000")
        assert output1.tobytes().decode('utf-8') == message
