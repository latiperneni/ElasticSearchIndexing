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

    def insert(self, index, payload, id, doc_type='_doc'):
        try:
            response_code = local.index(index=index, doc_type=doc_type, id=id, body=body)
            return response_code

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

    def search_by_scan(self, index, doc_type, size=1000, body={}):

        start_time = int(time.time())
        lst = []

        # Check index exists
        if not es.indices.exists(index=index):
            print("Index " + index + " not exists")
            exit()

        # Init scroll by search
        data = es.search(index=index, doc_type=doc_type, scroll='2m', size=size, body=body)

        # Get the scroll ID
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

        try:
            while scroll_size > 0:
                "Scrolling..."

                # Before scroll, process current batch of hits
                lst.extend([hit['_source'] for hit in data['hits']['hits']])

                data = es.scroll(scroll_id=sid, scroll='2m')

                # Update the scroll ID
                sid = data['_scroll_id']

                # Get the number of results that returned in the last scroll
                scroll_size = len(data['hits']['hits'])

        except Exception as e:
            print(e)
        finally:
            self.response = lst
            end_time = int(time.time())
            print('elastic activity, time taken = %s secs' % (end_time - start_time))


if __name__ == '__main__':
    # use any method needed to test eg: search_all(hosts,port,index)
    client = Search(hosts=hosts, port=port)

    # in case authentication needed
    # client = Elasticsearch([{'host': host, 'port': port}],http_auth=('username', 'password'), scheme="https", timeout=1000)

    # to do search all query
    try:
        payload = {}
        client.search(index=index, payload=payload)
        df_indexResponse = pd.DataFrame(client.response)

    except Exception as e:
        print('ERROR: exception during elastic search - %s' % e)

    # search by scan
    # to do batch searching when index has more than 10000 records
    try:
        payload = {}
        lst = client.search_by_scan(index=index, doc_type=doc_type, size=size, body=body)
        df = pd.DataFrame.from_records(lst)

    except Exception as e:
        print('ERROR: exception during elastic search - %s' % e)

    # delete querying
    try:
        client.delete_docs(index=index)
        df_indexResponse = pd.DataFrame(client.response)

    except Exception as e:
        print('ERROR: exception during elastic search - %s' % e)

    # inserting pandas dataframe to elastic search index
    try:
        # in case df is a pandas dataframe
        for index, row in df.iterrows():
            body = json.loads(row.to_json())
            local.index(index=index, doc_type='_doc', id=id, body=body)
    except Exception as e:
        print('ERROR: exception during elastic search - %s' % e)
