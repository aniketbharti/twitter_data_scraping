'''
@author: Souvik Das
Institute: University at Buffalo
'''

import os
import pysolr
import requests
import json

CORE_NAME = "IRF21P1"
AWS_IP = "3.135.224.20"
is_initial_setup = False


class Indexer:
    def __init__(self):
        self.solr_url = f'http://{AWS_IP}:8983/solr/'
        self.connection = pysolr.Solr(
            self.solr_url + CORE_NAME, always_commit=True, timeout=500000)

    def delete_core(self, core=CORE_NAME):
        print(os.system(
            'sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=core)))

    def create_core(self, core=CORE_NAME):
        print(os.system(
            'sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(
                core=core)))

    def do_initial_setup(self):
        self.delete_core()
        self.create_core()

    def create_documents(self, docs):
        status = self.connection.add(docs)
        print(status)

    def add_fields(self, data):
        status = requests.post(
            self.solr_url + CORE_NAME + '/schema', json=data, timeout=200)
        print(status)


if __name__ == "__main__":
    i = Indexer()
    if is_initial_setup:
        i.do_initial_setup()
        with open("./configs-files/solr-data-format.json", "r") as json_file:
            data = json.load(json_file)
        if data:
            i.add_fields(data)
