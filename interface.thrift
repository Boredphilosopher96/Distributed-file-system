exception CustomException {
  1: optional string message
}

// Interface between client and the file server
service ClientServerInterface {
    // If the client wants to read the latest version
    string read_from_file(1:string file_name) throws (1: CustomException custom_exception),

    // If the client wants to read the version held by the file server
    string read_file_from_node(1:string file_name) throws (1: CustomException custom_exception),

    // Only append operation is supported for write
    string write_to_file(1:string file_name, 2:string update) throws (1: CustomException custom_exception)
}

// Interface between 2 file servers
service ServerInterface {
    i32 get_file_version(1: string file_name) throws (1: CustomException custom_exception),

    string append_to_specific_file(1: string file_name, 2:string update, 3:i32 version_number) throws (1: CustomException custom_exception),

    string update_file_to_text(1:string file_name, 2:string new_file, 3:i32 version_number) throws (1: CustomException custom_exception),

    string read_file_from_node(1:string file_name) throws (1: CustomException custom_exception),

    // If the current file server is not the coordinator, it forwards the next 2 requests to the coordinator
    // Added this node_to_exclude in case we wanted to account for node failures, but since we are not handling it, it is kept empty for all requests
    string forwarded_read_from_file(1:string file_name, 2:string node_to_exclude) throws (1: CustomException custom_exception),

    string forwarded_write_to_file(1:string file_name, 2:string update, 3:string node_to_exclude) throws (1: CustomException custom_exception)
}