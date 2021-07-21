
# PlasmaFlight

A client-server based implementation for transfering plasma store objects. Objects can be passed through the client using the `memoryview` interface.

## Usage

### Local Node

```[python]
store = sp.Popen(["plasma_store", "-m", "100000000", "-s", "/tmp/plasma0"])
server = PlasmaFlightServer(
    location="grpc+tcp://localhost:5005",
    plasma_socket="/tmp/plasma0",
    tls_certificates=[],
    verify_client=False)


object_id = generate_sha1_object_id(b'my_key')
data = memoryview("你好".encode('utf-8'))

client = PlasmaFlightClient("/tmp/plasma0")
print(client.get(object_id).tobytes().decode('utf-8'))
>> 你好
```

### Remote Node

```[python]
store = sp.Popen(["plasma_store", "-m", "100000000", "-s", "/tmp/plasma1"])

client = PlasmaFlightClient("/tmp/plasma1")

# use local machine ip here
print(client.get(object_id, "10.1.1.1:5005").tobytes().decode('utf-8'))
>> 你好
```

### Plasma Store Synchronization

As demonstrated locally in tests in plasmaflight/tests/test_plasma_flight_synchronization.py:

![object-diagram](/docs/plasmaflight.png "Plasma Store Synchronization")

(the client will automatically check and cache fetched objects with the plasma store at the configured socket)
