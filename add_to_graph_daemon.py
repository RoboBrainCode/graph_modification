#!/usr/bin/python

import boto
import json
import traceback
import pymongo as pm
import datetime
from boto.sqs.message import RawMessage
from views import add_feed_to_graph
from bson import json_util
from bson.objectid import ObjectId

conn = boto.sqs.connect_to_region(
    "us-west-2", 
    aws_access_key_id='AKIAIDKZIEN24AUR7CJA', 
    aws_secret_access_key='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')

feed_queue = conn.create_queue('feed_queue')
feed_queue.set_message_class(RawMessage)

HOST = 'ec2-54-69-241-26.us-west-2.compute.amazonaws.com'
DBNAME = 'backend_test_deploy'
PORT = 27017
client = pm.MongoClient(HOST, PORT)
db = client[DBNAME]
json_feeds = db['json_feeds']

NUM_SECONDS_TO_POLL = 20
MSG_BATCH_SIZE = 10

def update_feed_in_db(json_feed):
    id_str = json_feed['_id'].__str__()
    json_feeds.update({ '_id': ObjectId(id_str) }, 
        { '$set': 
            {
                'added_to_graph_at': datetime.datetime.utcnow()
            }
        })

def main():
    while True:
        messages = feed_queue.get_messages(
            num_messages=MSG_BATCH_SIZE, wait_time_seconds=NUM_SECONDS_TO_POLL)
        for m in messages:
            json_feed = json.loads(m.get_body(), object_hook=json_util.object_hook)
            try:
                add_feed_to_graph(json_feed)
            except Exception, e:
                print traceback.format_exc()
                print json_feed
            else:
                update_feed_in_db(json_feed)
        feed_queue.delete_message_batch(messages)

if __name__ == '__main__':
    main()