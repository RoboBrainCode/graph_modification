#!/usr/bin/python
import os
import sys
import boto
import json
import traceback
from boto.sqs.message import RawMessage
from insertInWeaver import insertInWeaver
from bson import json_util

conn = boto.sqs.connect_to_region(
    "us-west-2", 
    aws_access_key_id='AKIAIDKZIEN24AUR7CJA', 
    aws_secret_access_key='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')

feed_queue = conn.create_queue('weaverFeed_queue')
feed_queue.set_message_class(RawMessage)

NUM_SECONDS_TO_POLL = 20
MSG_BATCH_SIZE = 10

PIDFILE = "/tmp/add_to_graph_daemon.pid"

def write_pid_to_file():
    pid = str(os.getpid())
    if os.path.isfile(PIDFILE):
        print "%s already exists, exiting." % PIDFILE
        sys.exit()
    else:
        file(PIDFILE, 'w').write(pid)

def main():
    write_pid_to_file()
    try:
        while True:
            queries = []
            feeds_to_update = []
            messages = feed_queue.get_messages(num_messages=MSG_BATCH_SIZE, 
                wait_time_seconds=NUM_SECONDS_TO_POLL)
            for m in messages:
                json_feed = json.loads(
                    m.get_body(), object_hook=json_util.object_hook)
                print insertInWeaver(json_feed)
                # feed_queue.delete_message(m)
                assert False
                # # if insertInWeaver(json_feed):
                # #     feed_queue.delete_message(m)
                # else:
                #     print 'Cannot insert the given message:'
                #     print m

    except Exception, e:
        print traceback.format_exc()

    finally:
        os.unlink(PIDFILE)
def test(json_feed):
    # print json_feed
    insertInWeaver(json_feed)

if __name__ == '__main__':
    main()
    # test({
    #     "username" : "arzav",
    #     "_id": "546e6a2f5caae434656bbc36",
    #     "feedtype" : "",
    #     "mediashow" : [ ],
    #     "text" : "#Simhat_Torah is a synonym of  #Rejoicing_in_the_Law",
    #     "hashtags" : " simhat_torah rejoicing_in_the_law", 
    #     "mediatype" : [ ],
    #     "source_url" : "http://wordnet.princeton.edu/",
    #     "source_text" : "WordNet",
    #     "mediamap" : [ ],
    #     "media" : [ ],
    #     "keywords": ["Simhat_Torah","Rejoicing_in_the_Law","synonym","wordnet"], 
    #     "upvotes" : 0, 
    #     "graphStructure": ["#same_synset: #0 -> #1", "#same_synset: #1 -> #0"]})