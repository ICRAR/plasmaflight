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
import hashlib
from io import BytesIO
from typing import List, Optional

import pyarrow
import pyarrow.flight as paf
import pyarrow.plasma as plasma

def generate_sha1_object_id(path: bytes) -> plasma.ObjectID:
    m = hashlib.sha1()
    m.update(path)
    id = m.digest()[0:20]
    return plasma.ObjectID(id)


class PlasmaFlightClient():
    def __init__(self, socket: str, scheme: str = "grpc+tcp", connection_args={}):
        """
        Args:
            socket (str): The socket of the local plasma store
            scheme (str, optional): [description]. Defaults to "grpc+tcp".
            connection_args (dict, optional): [description]. Defaults to {}.
        """
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
        self.plasma_client.put_raw_buffer(data, object_id)

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

    def exists(self, object_id: plasma.ObjectID, owner: Optional[str] = None) -> bool:
        if self.plasma_client.contains(object_id): return True
        if owner is not None:
            client = paf.FlightClient(f"{self._scheme}://{owner}", **self._connection_args)
            try:
                info = client.get_flight_info(paf.FlightDescriptor.for_path(object_id.binary().hex().encode('utf-8')))
                return True
            except:
                return False
        return False
