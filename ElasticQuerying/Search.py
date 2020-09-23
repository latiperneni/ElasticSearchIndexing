import time
from elasticsearch import Elasticsearch
import pandas as pd


class Search(object):
    ACTION_INDEX = 'index'
    ACTION_DELETE = 'delete'

    def __init__(self, hosts=[], port=None, timeout=30):
        start_t = time.time()
        self.client = Elasticsearch(hosts, port=port, timeout=timeout)
        self.response = []
        self.error = None
        self.count = 0
        print("Time Elapsed in elastic  instantiation --> {0} secs".format(time.time() - start_t))

    def search(self, index, payload, size=10000):
        try:
            size = size or 10000
            self.response = self.client.search(index=index, body=payload)
            self.count = self.response['hits']['total']
            self.response = self.response['hits']['hits']

        except Exception as e:
            self.error = str(e)
            print('error %s' % self.error)

    def delete_docs(self, index=None, query=()):
        try:
            self.response = self.client.delete_by_query(
                index=index,
                body=query
            )
            self.response = self.response

        except Exception as e:
            self.error = str(e)


# to do search all query
def search_all(client, index):
    try:
        payload = {}
        client.search(index=index, payload=payload)
        df_indexResponse = pd.DataFrame(client.response)

    except Exception as e:
        print('ERROR: exception during elastic search - %s' % e)


# to do batch searching when index has more than 10000 records
# post querying
# put querying

# delete querying
def delete(client, index):
    try:
        client.delete_docs(index=index)
        df_indexResponse = pd.DataFrame(client.response)

    except Exception as e:
        print('ERROR: exception during elastic search - %s' % e)


if __name__ == '__main__':
    # use any method needed to test eg: search_all(hosts,port,index)
    client = Search(hosts=hosts, port=port)
    search_all(client, index)
