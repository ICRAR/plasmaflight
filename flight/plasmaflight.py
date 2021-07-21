
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
    def __init__(self, socket: str):
        self._plasma_client = plasma.connect(socket)

    def put(self, object_id: plasma.ObjectID):
        #self._plasma_client.put(object_id)
        # TODO: server needs to rebuild tickets based on what is in the store since
        # this does get not broadcast

    def get(self, object_id: plasma.ObjectID):
        if self._plasma_client.exists(object_id):
            return self._plasma_client.get(object_id)
        else:
        


    def get_flight(self, flight_client,  object_id: plasma.ObjectID) -> paf.FlightStreamReader:
        descriptor = paf.FlightDescriptor.for_path(object_id.binary().hex().encode('utf-8'))
        info = flight_client.get_flight_info(descriptor)
        for endpoint in info.endpoints:
            for location in endpoint.locations:
                get_client = paf.FlightClient(location, **self._connection_args)
                return get_client.do_get(endpoint.ticket)

    def put(self, buffer: memoryview, object_id: plasma.ObjectID):
        descriptor = paf.FlightDescriptor.for_path(object_id.binary().hex())
        schema = pyarrow.schema([('data', pyarrow.binary(buffer.nbytes))])
        wrapper = pyarrow.record_batch([[buffer]], schema)
        writer, _ = flight_client.do_put(descriptor, schema)
        writer.write(wrapper)
        writer.close()

    def get(self, object_id: plasma.ObjectID) -> memoryview:
        reader = self.get_flight(object_id)
        table = reader.read_all()
        return BytesIO(table["data"][0].as_py()).getbuffer()


def connect(socket: str):
    return PlasmaFlightClient(socket)

