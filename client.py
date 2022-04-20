import sys

from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from thrift.transport import TSocket, TTransport
from utils import CONFIG
from interface import ClientServerInterface, ttypes


def get_client(node_id) -> ClientServerInterface.Client:
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

    return client


if __name__ == '__main__':
    command_file = sys.argv[1]
    with open(command_file) as commands:
        client = None
        for line in commands:
            command_line = line.rstrip().split(' ', maxsplit=2)
            if len(command_line) == 1:
                raise ttypes.CustomException("The input command format is not correct. Wrong number of arguments found")
            if command_line[0] == 'client':
                try:
                    client = get_client(command_line[1])
                except Exception as e:
                    raise ttypes.CustomException(f"Cannot connect to server {command_line[1]}. Please ensure it is a valid server")
            elif command_line[0] == "read":
                try:
                    print(client.read_from_file(command_line[1]))
                except ttypes.CustomException as e:
                    print(f"{e}")
                except Exception as e:
                    raise ttypes.CustomException(f"Error reading from file {command_line[1]}. Please ensure you have server")
            elif command_line[0] == "write":
                try:
                    client.write_to_file(command_line[1], command_line[2])
                except ttypes.CustomException as e:
                    print(f"{e}")
                except Exception as e:
                    raise ttypes.CustomException(
                        f"Error writing to file {command_line[1]}. Please ensure you have a valid server and your update is valid")
            else:
                raise ttypes.CustomException(
                    f"Invalid command. Only 3 commands allowed - client, read and write. Sent command {command_line[0]}")

