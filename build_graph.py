#! /usr/bin/env python

import pymongo as pm
from views import append_cypher_queries
from util import *
import sys, traceback

BATCH_SIZE = 100

def main(args):
    if len(args) > 1 and args[1] == '-n':
        all_feeds = json_feeds.find(
            { 'added_to_graph_at': { '$exists': False } }, timeout=False)
        print 'Adding only unadded feeds to graph.'
    else:
        all_feeds = json_feeds.find(timeout=False)
        print 'Adding all feeds to graph.'
        
    # To stop at a certain number of feeds even though feeds may be added during
    # build process:
    maxFeedsToAdd = all_feeds.count()
    count, errorCount = 0, 0
    queries = []
    feeds_to_update = []
    import pdb; pdb.set_trace()
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
                feeds_to_update.append(feed)
                if count % BATCH_SIZE == 0:
                    submit_batch(queries, feeds_to_update)
                    queries = []
                    feeds_to_update = []
                    print "%i feeds added to graph so far" % count
                if (count + errorCount) == maxFeedsToAdd:
                    break

        if len(queries) != 0:
            submit_batch(queries, feeds_to_update)

    finally:
        print "%i/%i feeds should have been added to the graph" % (maxFeedsToAdd, all_feeds.count())
        print "%i/%i feeds actually added to the graph" % (count, maxFeedsToAdd)
        print "%i/%i feeds contained errors" % (errorCount, maxFeedsToAdd)
        all_feeds.close()

if __name__ == "__main__":
    main(sys.argv)