import glob
import os
import random
import shutil
import socket
import timeit
from threading import Lock
from typing import List, Union

from thrift.TMultiplexedProcessor import TMultiplexedProcessor
from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

import utils
from interface import ClientServerInterface, ttypes
from interface import ServerInterface

CREATED_FILES = 'created_files'

"""
Assumptions:
 1. Update can only be done with append to files
 2. When you initialize the servers, there is an original copy of the file which all the other servers copy
 3. Update operations can only be append to files
 4. All files are in the project directory and they are in a folder set in config
"""


class ServerHandler:
    def __init__(self, node_id, node_info, coordinator, n_read_quorum, n_write_quorum, files_source: str):
        self.node_id = node_id
        self.coordinator = coordinator
        # Check if current node is coordinator.
        self.is_coordinator = self.node_id == self.coordinator
        # Get details of all nodes other than current node
        self.node_info: dict = node_info
        self.files_source: str = files_source[:-1] if files_source.endswith('/') else files_source
        # Maps containing file info for the node
        self.version_info = dict()
        self.file_list = dict()
        self.n_read_quorum: int = n_read_quorum
        self.n_write_quorum: int = n_write_quorum
        self.__initialize_files_and_version_info()

    def __get_file_path(self, file_name: str) -> str:
        return f"./{CREATED_FILES}/{file_name.split('.')[0]}_node{self.node_id}.txt"

    def __initialize_files_and_version_info(self):
        """
        On initialization of server, we make a copy of the file for this specific file server
        We also initialize file server info
        """
        source_text_files = glob.glob(glob.escape(self.files_source) + "/*.txt")
        try:
            if not os.path.exists('%s' % CREATED_FILES):
                os.makedirs(CREATED_FILES)
        except:
            raise ttypes.CustomException("Could not create folder for placing all generated files.\n"
                                         "Please give permission to program to create new folder")

        for file in source_text_files:
            file_name = file.split('/')[-1]
            # Make copies of files in source folder and name it to indicate which server the file belong to
            # Ex: if file is test123.txt, when node 2 is making a copy, it will name it as test123_node2.txt in the same folder
            shutil.copyfile(file, self.__get_file_path(file_name))
            self.version_info[file_name] = 1
            self.file_list[file_name] = Lock()

    def get_client(self, node_id) -> Union[ServerInterface.Client, 'ServerHandler']:
        if node_id == self.node_id:
            return self

        node_info = self.node_info[node_id]
        # Make socket
        socket = TSocket.TSocket(node_info[0], port=node_info[1])
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(socket)

        buffered_transport = TTransport.TBufferedTransport(socket)

        protocol = TMultiplexedProtocol.TMultiplexedProtocol(protocol, serviceName="server")

        # Create a client to use the protocol encoder
        client = ServerInterface.Client(protocol)

        # Connect!
        buffered_transport.open()

        return client

    def __get_eligible_nodes(self, node_to_exclude):
        nodes = set(self.node_info.keys())

        if node_to_exclude in nodes:
            nodes.remove(node_to_exclude)

        return list(nodes)

    def __get_read_quorum(self, node_to_exclude) -> List[str]:
        eligible_nodes = self.__get_eligible_nodes(node_to_exclude)
        return random.sample(eligible_nodes, self.n_read_quorum)

    def __get_write_quorum(self, node_to_exclude) -> List[str]:
        eligible_nodes = self.__get_eligible_nodes(node_to_exclude)
        return random.sample(eligible_nodes, self.n_write_quorum)

    def forwarded_read_from_file(self, file_name, node_to_exclude) -> str:
        try:
            if file_name not in self.file_list:
                raise ttypes.CustomException(message=f"Trying to read the file not in the system {file_name}")
            self.file_list[file_name].acquire()
            # If this is being handled by someone other than the coordinator, something has gone wrong!
            if not self.is_coordinator:
                raise ttypes.CustomException(message="This request was not supposed to be forwarded to this node.\n"
                                                     "Was only supposed to be forwarded to the coordinator node")

            read_quorum = self.__get_read_quorum(node_to_exclude)

            print(f"Read quorum to read file {file_name} assembled with members {read_quorum}")

            highest_version_number = -1
            highest_version_client = None
            node_with_highest_version = self.node_id
            for quorum_member in read_quorum:
                quorum_client: Union[ServerInterface.Client, 'ServerHandler'] = self.get_client(quorum_member)
                quorum_member_file_version = quorum_client.get_file_version(file_name)
                if quorum_member_file_version > highest_version_number:
                    highest_version_number = quorum_member_file_version
                    highest_version_client = quorum_client
                    node_with_highest_version = quorum_member

            print(f"Highest version for file {file_name} was found on {node_with_highest_version} "
                  f"- version number: {highest_version_number}")
            if highest_version_client is None:
                raise ttypes.CustomException(message="File not found")

            return highest_version_client.read_file_from_node(file_name)

        except Exception as e:
            print(str(e))
            return f"Something went wrong when trying to read from the file {file_name}"

        finally:
            self.file_list[file_name].release()

    def read_from_file(self, file_name: str) -> str:
        print(f"Trying to read the file {file_name}")
        start = timeit.default_timer()
        # If the current node is not the coordinator, just forward the request to the coordinator
        if not self.is_coordinator:
            coordinator_client: ServerInterface.Client = self.get_client(self.coordinator)
            result = coordinator_client.forwarded_read_from_file(file_name, '')
        else:
            result = self.forwarded_read_from_file(file_name, node_to_exclude="")
        end = timeit.default_timer()
        print(f"Time elapsed for reading from file {file_name} is {end - start} seconds")
        return result

    def read_file_from_node(self, file_name: str) -> str:
        with open(self.__get_file_path(file_name)) as f:
            return f.read()

    def get_file_version(self, file_name: str) -> int:
        print(f"Version number for file {file_name} : {self.version_info.get(file_name, -1)}")
        return self.version_info.get(file_name, -1)

    def append_to_specific_file(self, file_name: str, update: str, version_number: int) -> str:
        print(f"Updating file {file_name} to version {version_number}")
        with open(self.__get_file_path(file_name), 'a') as f:
            f.write(f"{update}\n")

        self.version_info[file_name] = version_number

        with open(self.__get_file_path(file_name), 'r') as f:
            return f.read()

    def update_file_to_text(self, file_name: str, new_file: str, version_number: int) -> str:
        print(f"Updating file {file_name} to version {version_number}")
        with open(self.__get_file_path(file_name), 'w') as f:
            f.write(new_file)

        self.version_info[file_name] = version_number

        with open(self.__get_file_path(file_name), 'r') as f:
            return f.read()

    def forwarded_write_to_file(self, file_name: str, update: str, node_to_exclude: str) -> str:
        try:
            if file_name not in self.file_list:
                raise ttypes.CustomException(message=f"Trying to write to file not in the system {file_name}")

            self.file_list[file_name].acquire()
            # If this is not the coordinator, don't handle this function
            if not self.is_coordinator:
                raise ttypes.CustomException(message="This request was not supposed to be forwarded to this node.\n"
                                                     "Was only supposed to be forwarded to the coordinator node")
            write_quorum_nodes = self.__get_write_quorum(node_to_exclude)
            print(f"Assembled write quorum to write to file {file_name} with members {write_quorum_nodes}")
            max_version_number = -1
            max_version_client = None
            node_with_max_id = self.node_id
            # Get the file server with the highest version file
            for quorum_node in write_quorum_nodes:
                quorum_client = self.get_client(quorum_node)
                quorum_node_file_version = quorum_client.get_file_version(file_name)
                if quorum_node_file_version > max_version_number:
                    max_version_number = quorum_node_file_version
                    max_version_client = quorum_client
                    node_with_max_id = quorum_node

            print(f"Highest version for file {file_name} was found on {node_with_max_id} "
                  f"- version number: {max_version_number}")

            if max_version_client is None:
                raise ttypes.CustomException(message="One of the members of write quorum does not have the file")

            # Ask the node with updated file to
            updated_file_content = max_version_client.append_to_specific_file(file_name, update, max_version_number + 1)

            for quorum_node in write_quorum_nodes:
                if quorum_node != node_with_max_id:
                    quorum_client = self.get_client(quorum_node)
                    quorum_client.update_file_to_text(file_name, updated_file_content, max_version_number + 1)

            return updated_file_content

        except ttypes.CustomException as e:
            print(str(e))
            raise e

        except Exception as e:
            print(str(e))
            return f"Something went wrong during execution of write to file : {file_name} - {update}"

        finally:
            if file_name in self.file_list:
                self.file_list[file_name].release()

    def write_to_file(self, file_name: str, string_to_append: str) -> str:
        print(f"Received request to write to file {file_name}")
        start = timeit.default_timer()
        if not self.is_coordinator:
            print(f"Forwarding request to coordinator {self.coordinator}")
            coordinator_client: Union[ServerInterface.Client, 'ServerHandler'] = self.get_client(self.coordinator)
            result = coordinator_client.forwarded_write_to_file(file_name, string_to_append, '')
        else:
            result = self.forwarded_write_to_file(file_name, string_to_append, node_to_exclude="")
        end = timeit.default_timer()
        print(f"Time elapsed to write to file {file_name} is {end - start} seconds")
        return result


if __name__ == '__main__':
    current_node = utils.CONFIG["currentNode"]
    if current_node not in utils.CONFIG["nodeInfo"]:
        raise ttypes.CustomException(message="The current node info is not in the configuration file")

    handler = ServerHandler(current_node, node_info=utils.CONFIG["nodeInfo"],
                            coordinator=utils.CONFIG["coordinator"],
                            n_read_quorum=utils.CONFIG["Nr"], n_write_quorum=utils.CONFIG["Nw"],
                            files_source=utils.CONFIG["filesSource"])

    current_node_info = utils.CONFIG["nodeInfo"][current_node]

    # If node set is set to auto, it will auto increment in config
    if utils.CONFIG["nodeSet"] == "auto":
        new_config = utils.CONFIG.copy()
        new_config["currentNode"] = str(int(current_node) + 1)
        utils.modify_config(new_config)

    # Because it implements multiple interfaces, register all the processors with name given in config file
    processor = TMultiplexedProcessor()
    processor.registerProcessor(
        "client_server",
        ClientServerInterface.Processor(handler)
    )
    processor.registerProcessor(
        "server",
        ServerInterface.Processor(handler)
    )

    transport = TSocket.TServerSocket(port=current_node_info[1])

    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print(f'Starting the server {current_node} at {socket.getfqdn(socket.gethostname())} port: {current_node_info[1]}')
    server.serve()
