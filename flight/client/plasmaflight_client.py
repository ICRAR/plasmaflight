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

import numpy as np
from pandas.core.frame import DataFrame
import pyarrow
import pyarrow.flight as paf
import pyarrow.plasma as plasma
import pyarrow.csv as csv

class PlasmaFlightClient():
    def __init__(self, host: str, port: int, scheme: str = "grpc+tcp", connection_args={}):
        self._flight_client = paf.FlightClient(
            f"{scheme}://{host}:{port}", **connection_args)
        self._connection_args = connection_args
    
    def list_flights(self):
        return self._flight_client.list_flights()

    def get_flight(self, object_id: plasma.ObjectID) -> paf.FlightStreamReader:
        print(object_id.binary().hex().encode('utf-8'))
        descriptor = paf.FlightDescriptor.for_path(object_id.binary().hex().encode('utf-8'))
        info = self._flight_client.get_flight_info(descriptor)
        for endpoint in info.endpoints:
            for location in endpoint.locations:
                get_client = paf.FlightClient(location, **self._connection_args)
                return get_client.do_get(endpoint.ticket)

    def put(self, buffer: memoryview, object_id: plasma.ObjectID):
        descriptor = paf.FlightDescriptor.for_path(object_id.binary().hex())
        schema = pyarrow.schema([('data', pyarrow.binary(buffer.nbytes))])
        wrapper = pyarrow.record_batch([[buffer]], schema)
        writer, _ = self._flight_client.do_put(descriptor, schema)
        writer.write(wrapper)
        writer.close()

    def get(self, object_id: plasma.ObjectID) -> memoryview:
        reader = self.get_flight(object_id)
        table = reader.read_all()
        return BytesIO(table["data"][0].as_py()).getbuffer()

