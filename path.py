import sys
import yaml

with open('user-config.yaml', 'r') as stream:
    data = yaml.load(stream, Loader=yaml.FullLoader)

sys.path.append(data['diarios_dir'])
db_dir = data['db_dir']
