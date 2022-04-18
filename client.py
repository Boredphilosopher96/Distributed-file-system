from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from thrift.transport import TSocket, TTransport
from utils import CONFIG
from interface import ClientServerInterface
if __name__ == '__main__':
    node_id = "5"
    node_info = CONFIG["nodeInfo"][node_id]
    # Make socket
    socket = TSocket.TSocket(node_info[0], port=node_info[1])
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(socket)

    buffered_transport = TTransport.TBufferedTransport(socket)

    protocol = TMultiplexedProtocol.TMultiplexedProtocol(protocol, serviceName="client_server")

    # Create a client to use the protocol encoder
    client = ClientServerInterface.Client(protocol)

    # Connect!
    buffered_transport.open()

    client.write_to_file('test2.txt', "aaaa")
    client.write_to_file('test.txt', "bbb")
    client.write_to_file('test.txt', "ccc")
    client.write_to_file('testy.txt', "aaaa")
    client.write_to_file('testy.txt', "bbb")
    client.write_to_file('test.txt', "ddd")
    client.write_to_file('test.txt', "eee")
    client.write_to_file('test.txt', "ffffff")
    client.write_to_file('test.txt', "gggggg")
    client.write_to_file('testy.txt', "ccc")
    print(client.read_from_file('test.txt'))
    print(client.read_from_file('testy.txt'))
