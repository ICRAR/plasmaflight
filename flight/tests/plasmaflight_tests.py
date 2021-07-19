
from dataclasses import make_dataclass
import subprocess as sp
import unittest
from io import BytesIO

import pandas
import numpy as np
import pyarrow
import pyarrow.plasma as plasma

from server.plasmaflight_server import PlasmaUtils, PlasmaFlightServer
from client.plasmaflight_client import PlasmaUtils, PlasmaFlightClient

class TestPlasmaflightClientServer(unittest.TestCase):
    def setUp(self):
        self._store = sp.Popen(["plasma_store", "-m", "100000000", "-s", "/tmp/plasma"])
        self.client = Plas

    def tearDown(self):
        self._store.terminate()
