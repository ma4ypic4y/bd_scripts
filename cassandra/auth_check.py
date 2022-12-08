import os
import yaml
import sys

from tqdm import tqdm

import random
from random import choices
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import names
from random_username.generate import generate_username



def read_yaml(path):
    with open(path, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


if __name__ == '__main__':
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')
    config = read_yaml(CONFIG_PATH)

    node_ips = ['10.210.99.65','10.210.99.66']
    auth_provider = PlainTextAuthProvider(username=config['cassandra']['username'], password=config['cassandra']['password'])
    cluster = Cluster(node_ips,auth_provider=auth_provider)
    session = cluster.connect()
    log = input("Enter your login: ")
    pwd = input("Enter your password: ")
    print(log,pwd)
    check = session.execute(f"select count (*)  from auth.table2 where login='{log}' and pwd='{pwd}';")
    print(check.current_rows[0].count)

    if check.current_rows[0].count==1:
        print('You are logged in cassandra cluster!')
