from elasticsearch import Elasticsearch
import csv

es = Elasticsearch()

with open('./fileName.csv') as f:
  index_name = 'indexName'
  doctype = 'type'
  reader = csv.reader(f)
  headers = []
  index = 0
  es.indices.delete(index=index_name, ignore=[400, 404])
  es.indices.create(index=index_name, ignore=400)
  es.indices.put_mapping(
      index=index_name,
      doc_type=doctype,
      ignore=400,
      body={
          doctype: {
              "properties": {
                  "prop1": {
                      "type": "string"
                      },
                  "prop2": {
                      "type": "string",
                  },
                  "prop3": {
                      "type": "string"
                  },
                  "prop4": {
                      "type": "string",
                  },
                  "prop5": {
                      "type": "string",
                  }
              }
          }
      }
  )
  for row in reader:
      try:
          if(index == 0):
              headers = row
          else:
              obj = {}
              for i, val in enumerate(row):
                  obj[headers[i]] = str(val)
                  print(obj)
              # put document into elastic search
              es.index(index=index_name,  doc_type=doctype, body=obj)
              print(obj)

      except Exception as e:
          print('error: ' + str(e) + ' in' + str(index))
      index = index + 1

f.close()