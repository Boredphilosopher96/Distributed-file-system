"""
This file was created to load the configuration files for the project
And to avoid writing repeated code
"""
import json
from interface.ttypes import CustomException

try:
    with open('config.json') as _config:
        CONFIG = json.load(_config)
        if CONFIG["maxNodes"] != len(CONFIG["nodeInfo"]):
            raise CustomException(message="Config not set properly. Max nodes doesn't match number of node info")
        elif ("coordinator" not in CONFIG) or (CONFIG["coordinator"] not in CONFIG["nodeInfo"]):
            raise CustomException(message="Coordinator info not configured properly")
        elif (CONFIG["Nw"] <= CONFIG["maxNodes"] / 2) or (CONFIG["Nr"] + CONFIG["Nw"] <= CONFIG["maxNodes"]):
            raise CustomException(message="Nr, Nw and Max nodes haven't been set properly\n"
                                          "Nw must be greater than N/2\n"
                                          "Nw + Nr must be greater than N")
        elif CONFIG["currentNode"] not in CONFIG["nodeInfo"] and CONFIG["currentNode"] != str(CONFIG["maxNodes"]+1):
            raise CustomException("This current node info is not correctly set in config file")
except FileNotFoundError as e:
    raise CustomException(message="Config file not found")
except Exception as e:
    raise e


def modify_config(new_config):
    with open('config.json', 'w') as _new_config_file:
        json.dump(new_config, _new_config_file, indent=4)
