#! /usr/bin/env python

import sys
import json
import re
from py2neo import cypher

GRAPH_DB_URL = "http://ec2-54-68-208-190.us-west-2.compute.amazonaws.com:7474"

def _normalize_handle_name(handle):
    return re.sub("\'", "", handle).lower()

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
    print edges_with_indices
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
    return "MERGE (c:Concept { id: %i, handle: '%s' }) RETURN c" % \
            (u_id, node_handle)

def _get_create_media_query(node_handle, u_id, node_idx, json_feed):
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

    return ("MERGE (m:Media:%s { id: %i, handle: '%s', mediapath: '%s' }) "
            "RETURN m") % (mediatype.title(), u_id, node_handle, mediapath)

def _add_node_to_graph(node_handle, node_idx, json_feed, tx):
    u_id = 0
    if (node_handle[0] != '$'):
        # u_id = get_unique_id(node_handle)
        # tx.append(_get_create_concept_query(node_handle, u_id))
        print _get_create_concept_query(node_handle, u_id)
    else:
        node_handle = node_handle[1:] # strip dollar sign
        # u_id = get_unique_id(node_handle)
        # tx.append(_get_create_media_query(node_handle, u_id, node_idx, json_feed))
        print _get_create_media_query(node_handle, u_id, node_idx, json_feed)
    return u_id

## TODO(arzav):
## 1. replace handle with unique ids for querying
## 2. add ids to edges?
def _add_edge_to_graph(edge_name, from_node_id, to_node_id, json_feed):
    edge_props = _get_edge_props(json_feed)
    # get rid of '' around keys since cypher doesn't like that
    props_str = re.sub("'(\\w+)':", r'\1:', str(edge_props))
    return ("MATCH (from { id: %i }), (to { id: %i }) "
            "CREATE (from)-[r:%s %s ]->(to)") % \
            (from_node_id, to_node_id, edge_name.upper(), props_str)

def add_feed_to_graph(json_feed):
    """ Public method supported in the api to add a json feed to the graph. """
    
    session = cypher.Session(GRAPH_DB_URL)
    tx = session.create_transaction()

    handles = [_normalize_handle_name(m.group(1)) \
                        for m in re.finditer('#([^ .]+)', json_feed['text'])]
    handle_graph_ids = []
    for i, handle in enumerate(handles):
        handle_graph_ids.append(_add_node_to_graph(handle, i, json_feed, tx))

    edges_with_indices = _get_edges_with_handle_indices(json_feed)

    for edge in edges_with_indices:
        from_node_id = handle_graph_ids[edge[1]]
        to_node_id = handle_graph_ids[edge[2]]
        print _add_edge_to_graph(edge[0], from_node_id, to_node_id, json_feed)

    # TODO(arzav): handle medias that are implicit/implied

