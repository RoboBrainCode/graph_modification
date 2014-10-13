#! /usr/bin/env python

import pymongo as pm
import sys
from views import add_feed_to_graph

HOST = 'ec2-54-69-241-26.us-west-2.compute.amazonaws.com'
DBNAME = 'backend_test_deploy'
PORT = 27017

def main(args):
    client = pm.MongoClient(HOST, PORT)
    db = client[DBNAME]
    json_feeds = db['json_feeds']
    all_feeds = json_feeds.find()
    count = 0
    for feed in all_feeds:
        feed['_id'] = feed['_id'].__str__()
        feed['created_at'] = feed['created_at'].isoformat()
        print feed
        add_feed_to_graph(feed)
        count += 1
        if count > 1: break

if __name__ == "__main__":
    main(sys.argv)