import configparser

def get_config_dict(path):
    if not hasattr(get_config_dict, "config_dict"):
        config = configparser.RawConfigParser()
        config.read(path)
        get_config_dict.config_dict = dict(config.items("API_Keys"))
    return get_config_dict.config_dict


if __name__ == "__main__":
    config_details = get_config_dict("example.cfg")
    print(config_details)
