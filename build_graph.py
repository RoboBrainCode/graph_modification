#! /usr/bin/env python

import pymongo as pm
from views import append_cypher_queries
from py2neo import neo4j
import sys

HOST = 'ec2-54-69-241-26.us-west-2.compute.amazonaws.com'
DBNAME = 'backend_test_deploy'
PORT = 27017
BATCH_SIZE = 500
GRAPH_DB_DATA_URL = "http://ec2-54-187-76-157.us-west-2.compute.amazonaws.com:7474/db/data/"
GRAPH_DB = neo4j.GraphDatabaseService(GRAPH_DB_DATA_URL)
WRITE_BATCH = neo4j.WriteBatch(GRAPH_DB)

def main():
    client = pm.MongoClient(HOST, PORT)
    db = client[DBNAME]
    json_feeds = db['json_feeds']
    all_feeds = json_feeds.find(timeout=False)
    count = 0
    queries = []

    try:
        for feed in all_feeds:
            feed['_id'] = feed['_id'].__str__()
            feed['created_at'] = feed['created_at'].isoformat()
            append_cypher_queries(feed, queries)
            count += 1
            if count % BATCH_SIZE == 0:
                for query in queries:
                    WRITE_BATCH.append_cypher(query)
                WRITE_BATCH.run()
                WRITE_BATCH.clear()
                queries = []

    finally:
        print "%i/%i feeds added to graph" % (count, all_feeds.count())
        all_feeds.close()

if __name__ == "__main__":
    main()