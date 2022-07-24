import json
import time
import pandas as pd
from elasticsearch import Elasticsearch
from subprocess import Popen


class ElasticSearch:
    def __init__(self, host='http://localhost:9200', elasticsearch_path=None):
        self.hosts = [host]
        self.index = 'health'
        if elasticsearch_path:
            self.__start_elasticsearch_server(elasticsearch_path)
        self.client = Elasticsearch(hosts=self.hosts)

    def delete_index(self):
        print("deleting the '{}' index.".format(self.index))
        res = self.client.indices.delete(index=self.index, ignore=[400, 404])
        print("Response for deleting from server: {}".format(res))

    def insert_data_and_create_index(self, folder_path):
        data = []
        for i in range(1, 8):
            with open(f'{folder_path}/namnak-p{i}.json', 'r', encoding="utf-8") as f:
                data.extend(json.loads(f.read()))
            with open(f'{folder_path}/hidoctor-p{i}.json', 'r', encoding="utf-8") as f:
                data.extend(json.loads(f.read()))
        print("Creating Index and Adding Data...")
        if self.client.indices.exists(index=self.index):
            self.delete_index()
        print("creating the '{}' index.".format(self.index))
        res = self.client.indices.create(index=self.index)
        print("Response for creating index from server: {}".format(res))
        print("bulk index the data")
        indexed_data = self.prepare_indexed_data(data)
        res = self.client.bulk(index=self.index, body=indexed_data, refresh=True)
        print("Errors: {}, Num of records indexed: {}".format(res["errors"], len(res["items"])))

    def __start_elasticsearch_server(self, elasticsearch_path):
        print("Starting Elasticsearch Server...")
        Popen(elasticsearch_path)
        time.sleep(30)

    def prepare_indexed_data(self, data):
        es_data = []
        for idx, record in enumerate(data):
            meta_dict = {
                "index": {
                    "_index": self.index,
                    "_id": idx
                }
            }
            es_data.append(meta_dict)
            es_data.append(record)
        return es_data

    def index_es_data(self, index, es_data):
        es_client = Elasticsearch(hosts=self.hosts)
        if es_client.indices.exists(index=index):
            print("deleting the '{}' index.".format(index))
            res = es_client.indices.delete(index=index)
            print("Response from server: {}".format(res))

        print("creating the '{}' index.".format(index))
        res = es_client.indices.create(index=index)
        print("Response from server: {}".format(res))

        print("bulk index the data")
        res = es_client.bulk(index=index, body=es_data, refresh=True)
        print("Errors: {}, Num of records indexed: {}".format(res["errors"], len(res["items"])))

    def show(self, results):
        res = []
        print()
        print('Results:')
        for ix, paper in enumerate(results):
            print(f'\n{ix}.', end='')
            print(f" {paper['_source']['title']}")
            print(f"Score: {paper['_score']}")
            res.append({'title': paper['_source']['title'],
                        'url': paper['_source']['url'],
                        'score': paper['_score']})
        print()
        return res

    def get_query(self, query, k=10):
        response = self.client.search(index=self.index, query={"match": {"text": query}})['hits']['hits'][:k]
        result = []
        for item in response:
            result.append((item['_source']['title'], item['_source']['link']))
        return result


# local_mode
elastic_model = ElasticSearch(elasticsearch_path="C:\\elasticsearch\\elasticsearch-7.3.2\\bin\\elasticsearch.bat")
# elastic_model.insert_data_and_create_index('dataset')
