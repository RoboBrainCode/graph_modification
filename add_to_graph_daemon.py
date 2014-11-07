#!/usr/bin/python

import boto
import json
from boto.sqs.message import RawMessage
from views import add_feed_to_graph

conn = boto.sqs.connect_to_region(
    "us-west-2", 
    aws_access_key_id='<aws access key>', 
    aws_secret_access_key='<aws secret key>')

feed_queue = conn.create_queue('feed_queue')
feed_queue.set_message_class(RawMessage)

NUM_HOURS_TO_POLL = 1
NUM_SECONDS_TO_POLL = NUM_HOURS_TO_POLL * 60 * 60
MSG_BATCH_SIZE = 10

def main():
    while True:
        messages = feed_queue.get_messages(
            num_messages=MSG_BATCH_SIZE, wait_time_seconds=NUM_SECONDS_TO_POLL)
        for m in messages:
            json_feed = json.loads(m.get_body())
            add_feed_to_graph(json_feed)
        feed_queue.delete_message_batch(messages)

if __name__ == '__main__':
    main()