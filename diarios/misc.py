import yaml


def get_user_config(key):
    with open('user-config.yaml', 'r') as stream:
        data = yaml.load(stream)
    return data[key]





