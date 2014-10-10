#! /usr/bin/env python

import sys
import json
import re
import ntpath
import datetime as dt
from py2neo import cypher

GRAPH_DB_URL = "http://ec2-54-68-208-190.us-west-2.compute.amazonaws.com:7474"
id_no = 20

def _normalize_handle_name(handle):
    return re.sub("\'", "", handle).lower()

def _filename_from_path(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def _get_timestamp():
    epoch = dt.datetime.utcfromtimestamp(0)
    return int((dt.datetime.now() - epoch).total_seconds() * 1000)

def _get_unique_id(node_handle):
    # return _get_timestamp()
    global id_no
    u_id = id_no
    id_no += 1
    return u_id

def _get_edges_with_handle_indices(json_feed):
    """ Returns array of tuples of the form (edge_name, 1, 2) where
    1 and 2 correspond to node/hashtag/handle indices in json_feed['text'].
    """
    edges_with_indices = [];
    for relation in json_feed['graphStructure']:
        relation = relation.strip()
        # remove the edge type part and only get the node ids
        edge_name_removed = re.sub('.*:','',relation)
        node_indices = \
            [m.group(1) for m in re.finditer(r'#(\d+)', edge_name_removed)]
        assert len(node_indices) == 2
        # match matches beginning of string only
        edge_name_match = re.match(r'#?([\w-]+):', relation)
        if edge_name_match is None: continue
        edge_name = _normalize_handle_name(edge_name_match.group(1))
        node0_idx = int(node_indices[0])
        node1_idx = int(node_indices[1])
        edges_with_indices.append((edge_name,node0_idx,node1_idx))
    return edges_with_indices 

def _get_edge_props(json_feed):
    props = {}
    props['keywords'] = []
    for k in json_feed['keywords']:
        props['keywords'].append(str(k)) # in case some keywords are not strings
    props['source_text'] = str(json_feed['source_text'])
    props['source_url'] = str(json_feed['source_url'])
    return props

def create_graph_elements(handles, media, edge_props):
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

def _get_create_concept_query(node_handle, u_id):
    return "MERGE (c:Concept { id: %i, handle: '%s' })" % (u_id, node_handle)

def _get_media_type_path(node_idx, json_feed):
    mediatype = ''
    mediapath = ''
    node_idx_str = str(node_idx)
    # find the first mediamap element that contains this node index
    for i, mediamap_elem in enumerate(json_feed['mediamap']):
        node_indices = set(re.findall(r'#(\d+)', mediamap_elem))
        if node_idx_str in node_indices:
            mediatype = json_feed['mediatype'][i]
            mediapath = json_feed['media'][i]
            break

    return (mediatype, mediapath)

def _get_create_media_query(node_handle, u_id, mediatype, mediapath):
    return ("MERGE (m:Media:%s { id: %i, handle: '%s', mediapath: '%s' })") % \
            (mediatype.title(), u_id, node_handle, mediapath)

def _add_media_node_to_graph(node_handle, mediatype, mediapath, tx):
    if node_handle is None:
        node_handle = _filename_from_path(mediapath)
    u_id = _get_unique_id(node_handle)
    tx.append(_get_create_media_query(node_handle, u_id, mediatype, mediapath))
    print _get_create_media_query(node_handle, u_id, mediatype, mediapath)
    return u_id


def _add_node_to_graph(node_handle, node_idx, json_feed, tx):
    u_id = 0
    if (node_handle[0] != '$'):
        u_id = _get_unique_id(node_handle)
        tx.append(_get_create_concept_query(node_handle, u_id))
        print _get_create_concept_query(node_handle, u_id)
    else:
        node_handle = node_handle[1:] # strip dollar sign
        mediatype, mediapath = _get_media_type_path(node_idx, json_feed)
        u_id = _add_media_node_to_graph(node_handle, mediatype, mediapath, tx)
    return u_id

def _add_edge_to_graph(edge_name, from_node_id, to_node_id, edge_props, tx):
    # get rid of '' around keys since cypher doesn't like that
    props_str = re.sub("'(\\w+)':", r'\1:', str(edge_props))
    return tx.append(
            ("MATCH (from { id: %i }), (to { id: %i }) "
            "CREATE (from)-[r:%s %s ]->(to)") % \
            (from_node_id, to_node_id, edge_name.upper(), props_str))

def add_feed_to_graph(json_feed):
    """ Public method supported in the api to add a json feed to the graph. """
    
    session = cypher.Session(GRAPH_DB_URL)
    tx = session.create_transaction()

    handles = [_normalize_handle_name(m.group(1)) \
                        for m in re.finditer('#([^ .]+)', json_feed['text'])]
    # Add nodes to graph
    handle_graph_ids = []
    for i, handle in enumerate(handles):
        handle_graph_ids.append(_add_node_to_graph(handle, i, json_feed, tx))

    edges_with_indices = _get_edges_with_handle_indices(json_feed)
    edge_props = _get_edge_props(json_feed)

    # Add edges to graph
    for edge in edges_with_indices:
        from_node_id = handle_graph_ids[edge[1]]
        to_node_id = handle_graph_ids[edge[2]]
        _add_edge_to_graph(edge[0], from_node_id, to_node_id, edge_props, tx)

    # Add remaining media nodes and edges to graph
    media_handle_set = set(
        [str(i) for i, handle in enumerate(handles) if handle[0] == '$'])
    for i, media_relation in enumerate(json_feed['mediamap']):
        related_nodes_set = set(re.findall(r'#(\d+)', media_relation))
        if len(media_handle_set & related_nodes_set) == 0: # #$ already handled
            media_node_id = _add_media_node_to_graph(
                    None, json_feed['mediatype'][i], json_feed['media'][i], tx)
            for node_idx in related_nodes_set:
                _add_edge_to_graph('has_media', handle_graph_ids[int(node_idx)], 
                                    media_node_id, edge_props, tx)
    tx.commit()


add_feed_to_graph({
    "feedtype": "Object Affordance",
    "text": "The position of a #Standing_human while using a #shoe is distributed as #$heatmap_12.",
    "source_text": "Hallucinating Humans",
    "source_url": "http://pr.cs.cornell.edu/hallucinatinghumans/",
    "mediashow": [
      "True",
      "True"
    ],
    "media": [
      "/home/ozan/ilcrf/images/shoe_.jpg",
      "/home/ozan/ilcrf/shoe_12_1.jpg_cr.jpg"
    ],
    "mediatype": [
      "Image",
      "image",
    ],
    "keywords": [
      "Human",
      "Affordance",
      "Object",
      "shoe",
      "Standing"
    ],
    "mediamap": [
      "#1",
      "#0#1#2"
    ],
    "graphStructure": [
      "#spatially_distributed_as: #1 ->#2",
      "#spatially_distributed_as: #0 ->#2",
      "can_use: #0 ->#1"
    ],
})

print '------------------'

add_feed_to_graph({
    "username": "hemakoppula",
    "feedtype": "Object Affordance",
    "mediashow": [
        "True",
        "True"
    ],
    "text": "The #sitting_human can #wear a #cap as shown in #$heatmap_1.",
    "created_at": "2014-09-23T01:34:10.187000",
    "hashtags": " sitting_human wear cap $heatmap_1.",
    "mediatype": [
        "Image",
        "Image"
    ],
    "source_url": "http://pr.cs.cornell.edu/anticipation/",
    "source_text": "Anticipation",
    "mediamap": [
        "#0#2",
        "#1#3"
    ],
    "media": [
        "hemakoppula/png/sitting_human/wear/cap/heatmap_1/cutting_frame.png",
        "hemakoppula/png/sitting_human/wear/cap/heatmap_1/cutting_result.png"
    ],
    "keywords": [
        "Human",
        "Affordance",
        "Object",
        "Cap"
    ],
    "upvotes": 0,
    "_id": "5420ce1242499173ddd83032",
    "graphStructure": [
        "can_perform_action: #0 ->#1",
        "can_use: #0 ->#2",
        "is_trajectory_of: #2 ->#3",
        "is_trajectory_of: #1 ->#3"
    ]
})