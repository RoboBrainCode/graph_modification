#!/usr/bin/python

import boto
import json
import traceback
from util import *
from boto.sqs.message import RawMessage
from views import append_cypher_queries
from bson import json_util

conn = boto.sqs.connect_to_region(
    "us-west-2", 
    aws_access_key_id='AKIAIDKZIEN24AUR7CJA', 
    aws_secret_access_key='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')

feed_queue = conn.create_queue('feed_queue')
feed_queue.set_message_class(RawMessage)

NUM_SECONDS_TO_POLL = 20
MSG_BATCH_SIZE = 10

def main():
    while True:
        queries = []
        feeds_to_update = []
        messages = feed_queue.get_messages(
            num_messages=MSG_BATCH_SIZE, wait_time_seconds=NUM_SECONDS_TO_POLL)
        print "Messages: ", messages
        for m in messages:
            json_feed = json.loads(m.get_body(), object_hook=json_util.object_hook)
            if feed_has_already_been_added(json_feed): continue
            temp_queries = []
            try:
                append_cypher_queries(json_feed, temp_queries)
            except Exception, e:
                print traceback.format_exc()
                print json_feed
            else:
                for q in temp_queries:
                    queries.append(q)
                feeds_to_update.append(json_feed)

        if len(queries) > 0:
            submit_batch(queries, feeds_to_update)
        feed_queue.delete_message_batch(messages)

if __name__ == '__main__':
    main()