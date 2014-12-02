#!/usr/bin/python

import pymongo as pm
import datetime
from bson.objectid import ObjectId
from py2neo import neo4j

HOST = 'ec2-54-148-208-139.us-west-2.compute.amazonaws.com'
DBNAME = 'backend_test_deploy'
PORT = 27017
client = pm.MongoClient(HOST, PORT)
db = client[DBNAME]
json_feeds = db['json_feeds']

GRAPH_DB_DATA_URL = "http://ec2-54-187-76-157.us-west-2.compute.amazonaws.com:7474/db/data/"
GRAPH_DB = neo4j.GraphDatabaseService(GRAPH_DB_DATA_URL)
WRITE_BATCH = neo4j.WriteBatch(GRAPH_DB)

def update_feeds_in_db(feeds):
    feed_ids = []
    for feed in feeds:
        feed_ids.append(ObjectId(feed['_id'].__str__()))

    json_feeds.update({ '_id': { '$in': feed_ids } }, 
        { '$set': 
            {
                'added_to_graph_at': datetime.datetime.utcnow()
            }
        })

def feed_has_already_been_added(json_feed):
    feed = json_feeds.find_one({ '_id': ObjectId(json_feed['_id'].__str__())})
    return feed != None and feed.get('added_to_graph_at', None) != None

def send_batch_to_graph(queries):
    for query in queries:
        WRITE_BATCH.append_cypher(query)
    WRITE_BATCH.run()
    WRITE_BATCH.clear()

def submit_batch(queries, feeds_to_update):
    send_batch_to_graph(queries)
    update_feeds_in_db(feeds_to_update)

if __name__ == '__main__':
    feed_has_already_been_added({ '_id': "543f08614249911f2af8baad" })
