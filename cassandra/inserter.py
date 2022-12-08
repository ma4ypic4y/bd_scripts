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


def pass_generator(pwd_length=8):
    digits = '0123456789'
    uppercase = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    lowercase = 'abcdefghijklmnopqrstuvwxyz'
    punctuation = '!#$%&*+-=?@^_'

    chars = ''

    for text, seq in (('Включить цифры',         digits     ),
                    ('Включить uppercase',     uppercase  ),
                    ('Включить lowercase',     lowercase  ),
                    ('Включить спец. символы', punctuation)):
        # if pwd_auto or (input(text + ' (y, n): ') == 'y'):
        chars += seq

    return ''.join(choices(chars, k=pwd_length))


def name():
    uppercase = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    lowercase = 'abcdefghijklmnopqrstuvwxyz'
    chars = ''

    pwd_length = random.randint(3,10)
    for text, seq in (('Включить uppercase',     uppercase  ),
                    ('Включить lowercase',     lowercase  )):
        # if pwd_auto or (input(text + ' (y, n): ') == 'y'):
        chars += seq

    return ''.join(choices(chars, k=pwd_length))


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


    n = int(sys.argv[1])

    if n:
        for _ in tqdm(range(n)):
            session.execute(
                f"INSERT INTO auth.table (login, name, pwd, time_id, iq)\
                VALUES ('{generate_username()[0]}', '{names.get_full_name()}', '{pass_generator()}', now(), {random.randint(-5,150)})")
