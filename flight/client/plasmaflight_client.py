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

"""An example Flight CLI client."""

import argparse
import sys

import hashlib
from io import BytesIO
from typing import List, Optional

import numpy as np
from pandas.core.frame import DataFrame
import pyarrow
import pyarrow.flight as paf
import pyarrow.plasma as plasma
import pyarrow.csv as csv

class PlasmaFlightClient():
    def __init__(self, socket: str, remotes: Optional[List[str]] = None, scheme: str = "grpc+tcp", connection_args={}):
        self.plasma_client = plasma.connect(socket)
        self._remotes = remotes
        self._scheme = scheme
        self._connection_args = connection_args

    def list_flights(self):
        for remote in self._remotes:
            flight_client = paf.FlightClient(
                f"{self._scheme}://{remote}", **self._connection_args)
            yield flight_client.list_flights()

    def get_flight(self, object_id: plasma.ObjectID, location: Optional[str]) -> paf.FlightStreamReader:
        descriptor = paf.FlightDescriptor.for_path(object_id.binary().hex().encode('utf-8'))
        if location is not None:
            flight_client = paf.FlightClient(f"{self._scheme}://{location}", **self._connection_args)
            info = flight_client.get_flight_info(descriptor)
            for endpoint in info.endpoints:
                for location in endpoint.locations:
                    return flight_client.do_get(endpoint.ticket)
        else:
            raise Exception()

    def put(self, data: memoryview, object_id: plasma.ObjectID):
        buffer = memoryview(self.plasma_client.create(object_id, data.nbytes))
        #buffer[i] = data[i]
        # slice assignment does not work with mutable <= immutable.. 
        for i in range(0, data.nbytes):
            buffer[i] = data[i]
        self.plasma_client.seal(object_id)

    def get(self, object_id: plasma.ObjectID, owner: Optional[str] = None) -> memoryview:
        #first check if the local store contains the object
        if self.plasma_client.contains(object_id):
            [buf] = self.plasma_client.get_buffers([object_id])
            return memoryview(buf)
        elif owner is not None:

            flight_client = paf.FlightClient(f"{self._scheme}://{owner}", **self._connection_args)
            print(object_id)
            list_flights(flight_client)

            reader = self.get_flight(object_id, owner)
            table = reader.read_all()
            return BytesIO(table["data"][0].as_py()).getbuffer()
        else:
            raise Exception()


def list_flights(client: paf.FlightClient):
    print('Flights\n=======')
    for flight in client.list_flights():
        descriptor = flight.descriptor
        if descriptor.descriptor_type == paf.DescriptorType.PATH:
            print("Path:", descriptor.path)
        elif descriptor.descriptor_type == paf.DescriptorType.CMD:
            print("Command:", descriptor.command)
        else:
            print("Unknown descriptor type")

        print("Total records:", end=" ")
        if flight.total_records >= 0:
            print(flight.total_records)
        else:
            print("Unknown")

        print("Total bytes:", end=" ")
        if flight.total_bytes >= 0:
            print(flight.total_bytes)
        else:
            print("Unknown")

        print("Number of endpoints:", len(flight.endpoints))
        print("Schema:")
        print(flight.schema)
        print('---')