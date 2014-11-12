#! /usr/bin/env python

import pymongo as pm
from views import append_cypher_queries
from py2neo import neo4j
import sys, traceback

HOST = 'ec2-54-69-241-26.us-west-2.compute.amazonaws.com'
DBNAME = 'backend_test_deploy'
PORT = 27017
BATCH_SIZE = 500
GRAPH_DB_DATA_URL = "http://ec2-54-187-76-157.us-west-2.compute.amazonaws.com:7474/db/data/"
GRAPH_DB = neo4j.GraphDatabaseService(GRAPH_DB_DATA_URL)
WRITE_BATCH = neo4j.WriteBatch(GRAPH_DB)

def send_batch_to_graph(queries):
    for query in queries:
        WRITE_BATCH.append_cypher(query)
    WRITE_BATCH.run()
    WRITE_BATCH.clear()

def main():
    client = pm.MongoClient(HOST, PORT)
    db = client[DBNAME]
    json_feeds = db['json_feeds']
    all_feeds = json_feeds.find(timeout=False)
    # To stop at a certain number of feeds even though feeds may be added during
    # build process:
    maxFeedsToAdd = all_feeds.count()
    count, errorCount = 0, 0
    queries = []

    try:
        for feed in all_feeds:
            feed['_id'] = feed['_id'].__str__()
            feed['created_at'] = feed['created_at'].isoformat()
            temp_queries = []

            try:
                append_cypher_queries(feed, temp_queries)
            except Exception, e:
                print traceback.format_exc()
                print feed
                errorCount += 1
            else:
                count += 1
                for q in temp_queries:
                    queries.append(q)
                if count % BATCH_SIZE == 0:
                    send_batch_to_graph(queries)
                    queries = []
                    print "%i feeds added to graph so far" % count
                if (count + errorCount) == maxFeedsToAdd:
                    break
            
        if len(queries) != 0:
            send_batch_to_graph(queries)

    finally:
        print "%i/%i feeds should have been added to the graph" % (maxFeedsToAdd, all_feeds.count())
        print "%i/%i feeds actually added to the graph" % (count, maxFeedsToAdd)
        print "%i/%i feeds contained errors" % (errorCount, maxFeedsToAdd)
        all_feeds.close()

if __name__ == "__main__":
    main()