#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Import BrainFeed as JSON and add it to a
#                  neo4j graph.  The input is in the
#                  form of a JSON array in which each JSON object
#                  conforms to the predefined client schema.
# 
#        Created:  2014-09-01 13:14:17
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
#                  Ozan Sener, os79@cornell.edu
#                  Arzav Jain, arzavj@stanford.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
import json
import re
from py2neo import cypher


def get_indices(entry):
    indices = [];
    for relations in entry['graphStructure']:
    rel_ = re.sub('.*:','',relations) #remove the edge type part and only get the node ids
        rel_idx = [m.start()+1 for m in re.finditer('#', rel_)] #Iterate over the node ids
        edge_name = (re.sub('\s.*','',re.sub(':,*','',relations))) #Get the edge type
        node0_idx = int(rel_[rel_idx[0]])
        node1_idx = int(rel_[rel_idx[1]])
        indices.append((edge_name,node0_idx,node1_idx))
    return indices 


def get_handles(entry):
    hashes = [m.group(0)[1:] for m in re.finditer('#[^ .]+', entry['text'])]
    indices = get_indices(entry)
    handles = []
    for idx in indices:
        edge_handle =  idx[0] #str(hashes[idx[0]])
        node0_handle = str(hashes[idx[1]])
        node1_handle = str(hashes[idx[2]])
    node1_handle=re.sub("\'","",node1_handle)
    node0_handle=re.sub("\'","",node0_handle)
        handles.append((edge_handle,node0_handle,node1_handle))
    return handles

# This function need to be re-written, currently it is ignoring all the media. 
def get_media(entry):
    assert len(entry['media']) == len(entry['mediamap'])
    edge_media = ['1']
    node0_media = ['1']
    node1_media = ['1']
    return (edge_media, node0_media, node1_media)


def get_edge_props(entry):
    props = {}
    props['keywords'] = []
    for k in entry['keywords']:
        props['keywords'].append(str(k))
    props['source_text'] = str(entry['source_text'])
    props['source_url'] = str(entry['source_url'])
    return props


def append_to_media(media, props):
    if 'media' in props:
        for m in props['media']:
            mstr = str(m)
            if mstr not in media:
                media.append(mstr)

def create_graph_elements(handles, media, edge_props):
    session = cypher.Session("http://ec2-54-68-208-190.us-west-2.compute.amazonaws.com:7474")
    tx = session.create_transaction()

    tx.append("MERGE (n { handle:'" + handles[1] + "' }) "
              "RETURN n.media")
    tx.append("MERGE (n { handle:'" + handles[2] + "' }) "
              "RETURN n.media")

    results = tx.execute()
    assert len(results) == 2
    assert len(results[0]) == 1
    assert len(results[1]) == 1
    node0_props = results[0][0].values[0]
    node1_props = results[1][0].values[0]

    edge_props_str = ''
    edge_props_str += "{ handle: '" + handles[0] + "'"
    edge_props_str += ', keywords: ' + str(edge_props['keywords'])
    edge_props_str += ", source_text: '" + edge_props['source_text'] + "'"
    edge_props_str += ", source_url: '" + edge_props['source_url'] + "'"
    edge_props_str += ' }'

    tx.append("MATCH (a),(b) "
              "WHERE a.handle = '" + handles[1] + "' AND b.handle ='" + handles[2] + "' "
              "CREATE (a)-[r:EDGE " + edge_props_str + "]->(b)")

    tx.commit()


import pymongo as pm
from datetime import datetime
import sys
def main(args):
    host = 'ec2-54-186-47-107.us-west-2.compute.amazonaws.com' #setfile.DATABASES['default']['HOST']
    dbname = 'backend_test_deploy'
    port = 27017
    client = pm.MongoClient(host,port)
    db = client[dbname]
    json_feeds = db['json_feeds']
    all_feeds = json_feeds.find()

    for feed in all_feeds:
        feed['_id'] = feed['_id'].__str__()
        feed['created_at'] = feed['created_at'].isoformat()
    entry = feed
        print entry
    handles = get_handles(entry)
        for handle in handles:
            print handle
            create_graph_elements(handle, get_media(entry), get_edge_props(entry))


if __name__ == "__main__":
    main(sys.argv)
