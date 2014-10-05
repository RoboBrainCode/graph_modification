import sys
import json
import re
from py2neo import cypher

GRAPH_DB_URL = "http://ec2-54-68-208-190.us-west-2.compute.amazonaws.com:7474"

def get_edges_with_handle_indices(entry):
    """ Returns array of tuples of the form (edge_name, 1, 2) where
    1 and 2 correspond to node/hashtag/handle indices in entry['text'].
    """
    edges_with_indices = [];
    for relation in entry['graphStructure']:
        relation = relation.strip()
        edge_name_removed = re.sub('.*:','',relation) #remove the edge type part and only get the node ids
        node_indices = [m.group(1) for m in re.finditer(r'#(\d+)', edge_name_removed)]
        assert len(node_indices) == 2
        edge_name = re.match(r'#([\w-]+)', relation).group(1) #match matches beginning of string only
        node0_idx = int(node_indices[0])
        node1_idx = int(node_indices[1])
        edges_with_indices.append((edge_name,node0_idx,node1_idx))
    return edges_with_indices 


def get_edges_with_handles(entry):
    hashes = [m.group(0)[1:] for m in re.finditer('#[^ .]+', entry['text'])]
    edges_with_indices = get_edges_with_handle_indices(entry)
    edges_with_handles = []
    for idx in edges_with_indices:
        edge_handle =  idx[0] #str(hashes[idx[0]])
        node0_handle = str(hashes[idx[1]])
        node1_handle = str(hashes[idx[2]])
        node1_handle=re.sub("\'","",node1_handle)
        node0_handle=re.sub("\'","",node0_handle)
        edges_with_handles.append((edge_handle,node0_handle,node1_handle))
    return edges_with_handles

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
    print "Handles: ", handles
    print "Media: ", media
    print "Edge_props: ", edge_props
    return 1
    session = cypher.Session(GRAPH_DB_URL)
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

get_edges_with_handles({
    'text': 'The position of a #Standing_human while using a #shoe is distributed as #heatmap_12.',
    "graphStructure": [
      "#spatially_distributed_as: #1 ->#2",
      "#spatially_distributed_as: #0 ->#2",
      "can_use: #0 ->#1"
    ]})
