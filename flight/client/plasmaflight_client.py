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

def generate_sha1_object_id(path: bytes) -> plasma.ObjectID:
    m = hashlib.sha1()
    m.update(path)
    id = m.digest()[0:20]
    return plasma.ObjectID(id)


class PlasmaFlightClient():
    def __init__(self, socket: str, scheme: str = "grpc+tcp", connection_args={}):
        self.plasma_client = plasma.connect(socket)
        self._scheme = scheme
        self._connection_args = connection_args

    def list_flights(self, location: str):
        flight_client = paf.FlightClient(
            f"{self._scheme}://{location}", **self._connection_args)
        return flight_client.list_flights()

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
        buffer[:] = memoryview(pyarrow.py_buffer(data))[:]
        self.plasma_client.seal(object_id)

    def get(self, object_id: plasma.ObjectID, owner: Optional[str] = None) -> memoryview:
        if self.plasma_client.contains(object_id):
            # first check if the local store contains the object
            [buf] = self.plasma_client.get_buffers([object_id])
            return memoryview(buf)
        elif owner is not None:
            # fetch from the specified owner
            reader = self.get_flight(object_id, owner)
            table = reader.read_all()
            output = BytesIO(table.column(0)[0].as_py()).getbuffer()
            #cache output
            self.put(output, object_id)
            return output
        else:
            raise KeyError("ObjectID not found", object_id)
