# Import modules
import json


def read_config_file(config_file_path):
    """
    Read a config file
    @param config_file_path: The path to the config file
    @return: The config file
    """
    import json
    config_file = open(config_file_path, 'r')
    config = json.load(config_file)
    config_file.close()
    return config
