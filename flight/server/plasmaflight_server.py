# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""An example Flight Python server."""

from typing import Any, Dict, Tuple, Optional, List
from overrides import overrides
from dataclasses import dataclass, astuple

import sys
import subprocess
import argparse
import ast
import threading
import time

import pandas
import pyarrow
import pyarrow.flight as flight
import pyarrow.plasma as plasma


@dataclass(unsafe_hash=True)
class FlightKey:
    descriptor_type: int = flight.DescriptorType.UNKNOWN.value
    command: Optional[str] = None
    path: Tuple[bytearray] = tuple()

    def __init__(self, descriptor_type, command, path):
        self.descriptor_type = descriptor_type
        self.command = command
        self.path = path

    def __iter__(self):
        return iter(astuple(self))

    def __getitem__(self, item):
        return getattr(self, item)


class PlasmaUtils:
    @classmethod
    def put_dataframe(cls, client: plasma.PlasmaClient, data: pandas.DataFrame, object_id: plasma.ObjectID):
        record_batch: pyarrow.RecordBatch = pyarrow.RecordBatch.from_pandas(data)
        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, record_batch.schema)
        stream_writer.write_batch(record_batch)
        stream_writer.close()
        data_size = mock_sink.size()
        buf = client.create(object_id, data_size)
        stream = pyarrow.FixedSizeBufferWriter(buf)
        stream_writer = pyarrow.RecordBatchStreamWriter(stream, record_batch.schema)
        stream_writer.write_batch(record_batch)
        stream_writer.close()
        client.seal(object_id)

    @classmethod
    def get_dataframe(cls, client: plasma.PlasmaClient, object_id: plasma.ObjectID) -> pandas.DataFrame:
        [buf] = client.get_buffers([object_id])
        buffer = pyarrow.BufferReader(buf)
        reader = pyarrow.RecordBatchStreamReader(buffer)
        record_batch = reader.read_next_batch()
        return record_batch.to_pandas()

    @classmethod
    def put_tensor(cls, client: plasma.PlasmaClient, data: pyarrow.Tensor, object_id: plasma.ObjectID):
        data_size = pyarrow.ipc.get_tensor_size(data)
        buffer: memoryview = client.create(object_id, data_size)
        stream = pyarrow.FixedSizeBufferWriter(buffer)
        pyarrow.ipc.write_tensor(data, stream)
        client.seal(object_id)

    @classmethod
    def get_tensor(cls, client: plasma.PlasmaClient, object_id: plasma.ObjectID) -> pyarrow.Tensor:
        [buf] = client.get_buffers([object_id])
        reader = pyarrow.BufferReader(buf)
        return pyarrow.ipc.read_tensor(reader)

    @classmethod
    def put_table(cls, client: plasma.PlasmaClient, data: pyarrow.Table, object_id: plasma.ObjectID):
        # TODO: calculate table size?
        # buf = self.plasma_client.create(object_id, data.nbytes * data.num_rows * data.num_columns)
        # stream = pyarrow.FixedSizeBufferWriter(buf)
        # stream_writer = pyarrow.RecordBatchStreamWriter(stream, data.schema)
        # stream_writer.write_table(data)
        # stream_writer.close()
        pass


class FlightServer(flight.FlightServerBase):
    def __init__(self, host="localhost", location:str=None,
                 tls_certificates:list=None, verify_client:bool=False,
                 root_certificates:bytes=None, auth_handler:flight.ServerAuthHandler=None):
        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        self.host = host
        #self.plasma_server = subprocess.Popen(["plasma_store", "-m", "10000000", "-s", "/tmp/plasma"])#, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.plasma_client = plasma.connect("/tmp/plasma")
        self.flights: Dict[FlightKey, plasma.ObjectID] = {}
        self.tls_certificates = tls_certificates

        # populate flights with existing plasma store
        # store = self.plasma_client.list()
        # for key, value in store.items():
        #     print("key", key)
        #     print("value", value)
        #self.flights[key] = table

    def __del__(self):
        #self.plasma_server.communicate()
        pass

    @classmethod
    def descriptor_to_key(cls, descriptor: flight.FlightDescriptor) -> FlightKey:
        return FlightKey(
            descriptor.descriptor_type.value,
            descriptor.command,
            tuple(descriptor.path or tuple())
        )

    def _make_flight_table_info(self, key: FlightKey, descriptor: flight.FlightDescriptor, table: pyarrow.Table) -> flight.FlightInfo:
        """
        Creates a flight ticket for a table. The flight client will receive
        the table schema and number of rows.
        """
        if self.tls_certificates:
            location = flight.Location.for_grpc_tls(
                self.host, self.port)
        else:
            location = flight.Location.for_grpc_tcp(
                self.host, self.port)
        endpoints = [flight.FlightEndpoint(repr(key), [location]), ]

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(
            mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return flight.FlightInfo(table.schema,
                                         descriptor, endpoints,
                                         table.num_rows, data_size)

    def _make_flight_unknown_plasma_info(self, key: FlightKey, descriptor: flight.FlightDescriptor, data: plasma.ObjectID) -> flight.FlightInfo:
        """
        Creates a flight ticket of unknown plasma data. The flight client is required
        to manage the typings of the flight object.
        """
        if self.tls_certificates:
            location = flight.Location.for_grpc_tls(
                self.host, self.port)
        else:
            location = flight.Location.for_grpc_tcp(
                self.host, self.port)
        endpoints = [flight.FlightEndpoint(repr(key), [location]), ]
        data_size = self.plasma_client.list()[data]['data_size']
        return flight.FlightInfo(pyarrow.schema([('data', pyarrow.null())]), descriptor, endpoints, 1, data_size)

    def _make_flight_info(self, key: FlightKey, descriptor: flight.FlightDescriptor, data: Any) -> flight.FlightInfo:
        if isinstance(data, pyarrow.Table):
            return self._make_flight_table_info(key, descriptor, data)
        elif isinstance(data, plasma.ObjectID):
            return self._make_flight_unknown_plasma_info(key, descriptor, data)
        else:
            raise Exception("unknown flight object")

    def list_flights(self, context, criteria) -> flight.FlightInfo:
        for key, object_id in self.flights.items():
            if key.command is not None:
                descriptor = \
                    flight.FlightDescriptor.for_command(key.command)
            else:
                descriptor = flight.FlightDescriptor.for_path(*key.path)

            yield self._make_flight_info(key, descriptor, object_id)

    def get_flight_info(self, context, descriptor: flight.FlightDescriptor):
        print("plasma", self.plasma_client.list())
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            object_id = self.flights[key]
            # TODO: check plasma_id exists: table = self.plasma_client.get(key.path)
            return self._make_flight_info(key, descriptor, object_id)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor: flight.FlightDescriptor, reader: flight.MetadataRecordBatchReader, writer: flight.MetadataRecordBatchWriter):
        key = FlightServer.descriptor_to_key(descriptor)
        data = reader.read_all()
        print(type(data))

        # move to plasma store
        print(key.path[0].decode('utf-8'))
        object_id = plasma.ObjectID(bytes.fromhex(key.path[0].decode('utf-8')))
        self.flights[key] = object_id
        
        if isinstance(data, pyarrow.Table) and data.shape == (1,1):
            # store only the data of a unit table 
            self.plasma_client.put(data["data"][0].as_buffer(), object_id)
        elif isinstance(data, str) or isinstance(data, int):
            self.plasma_client.put(data, object_id)
        elif isinstance(data, pyarrow.Tensor):
            PlasmaUtils.put_tensor(self.plasma_client, data, object_id)
        elif isinstance(data, pandas.DataFrame):
            PlasmaUtils.put_dataframe(self.plasma_client, data, object_id)
        elif isinstance(data, pyarrow.Table):
            PlasmaUtils.put_dataframe(self.plasma_client, data.to_pandas(), object_id)
        else:
            raise Exception("unrecognized data type")

    def do_get(self, context, ticket: flight.Ticket):
        # TODO: literal eval does not work with dataclass
        # key = ast.literal_eval(ticket.ticket.decode())
        key = eval(ticket.ticket.decode())

       # plasma memory
        object_id = plasma.ObjectID(bytes.fromhex(key.path[0].decode('utf-8')))
        # convert back to table
        table = pyarrow.Table.from_pandas(PlasmaUtils.get_dataframe(self.plasma_client, object_id))
        return flight.RecordBatchStream(table)

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("shutdown", "Shut down this server."),
        ]

    def do_action(self, context, action):
        if action.type == "clear":
            raise NotImplementedError(
                "{} is not implemented.".format(action.type))
        elif action.type == "healthcheck":
            pass
        elif action.type == "shutdown":
            yield flight.Result(pyarrow.py_buffer(b'Shutdown!'))
            # Shut down on background thread to avoid blocking current
            # request
            threading.Thread(target=self._shutdown).start()
        else:
            raise KeyError("Unknown action {!r}".format(action.type))

    def _shutdown(self):
        """Shut down after a delay."""
        print("Server is shutting down...")
        time.sleep(2)
        self.shutdown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost",
                        help="Address or hostname to listen on")
    parser.add_argument("--port", type=int, default=5005,
                        help="Port number to listen on")
    parser.add_argument("--tls", nargs=2, default=None,
                        metavar=('CERTFILE', 'KEYFILE'),
                        help="Enable transport-level security")
    parser.add_argument("--verify_client", type=bool, default=False,
                        help="enable mutual TLS and verify the client if True")

    args = parser.parse_args()
    tls_certificates = []
    scheme = "grpc+tcp"
    if args.tls:
        scheme = "grpc+tls"
        with open(args.tls[0], "rb") as cert_file:
            tls_cert_chain = cert_file.read()
        with open(args.tls[1], "rb") as key_file:
            tls_private_key = key_file.read()
        tls_certificates.append((tls_cert_chain, tls_private_key))

    location = "{}://{}:{}".format(scheme, args.host, args.port)

    server = FlightServer(args.host, location,
                          tls_certificates=tls_certificates,
                          verify_client=args.verify_client)
    print("Serving on", location)
    server.serve()


if __name__ == '__main__':
    main()
