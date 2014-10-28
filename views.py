#! /usr/bin/env python

import sys
import re
import ntpath
import datetime as dt
import hashlib
import ConfigParser
import os
from py2neo import cypher

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + 
                '/../learning_plugins')
# from lemmatizer import lemmatize
import spellcheck

GRAPH_DB_URL = "http://ec2-54-187-76-157.us-west-2.compute.amazonaws.com:7474"
tx = cypher.Session(GRAPH_DB_URL).create_transaction()

CONFIG = ConfigParser.ConfigParser()
CONFIG.read(os.path.dirname(os.path.abspath(__file__)) + '/config.ini')
NormalizationSection = 'HandleNameNormalizations'

def _normalize_handle_name(handle):
    if handle == None or len(handle) == 0: return handle
    normHandle = handle
    if CONFIG.getboolean(NormalizationSection, 'spellcheck'):
        normHandle = spellcheck.correct(normHandle)
    normHandle = re.sub("[\'\"]", "", normHandle).strip().lower()
    # if CONFIG.getboolean(NormalizationSection, 'lemmatize'):
    #     normHandle = lemmatize(normHandle)
    return normHandle.encode('utf8')

def _filename_from_path(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def _get_timestamp():
    epoch = dt.datetime.utcfromtimestamp(0)
    return int((dt.datetime.now() - epoch).total_seconds() * 1000)

def _get_unique_id(node_handle):
    m = hashlib.md5(node_handle.encode())
    return int(str(int(m.hexdigest(), 16))[0:16])

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

def _get_create_concept_query(node_handle, u_id, feed_id):
    return ("MERGE (c:Concept { handle: '%s' }) "
            "ON CREATE SET c.id = %i, c.created_at = timestamp(), "
            "c.feed_ids = ['%s'] "
            "ON MATCH SET c.feed_ids = c.feed_ids + ['%s']") % \
            (node_handle, u_id, feed_id, feed_id)

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

def _get_create_media_query(node_handle, u_id, mediatype, mediapath, feed_id):
    return ("MERGE (m:Media:%s { handle: '%s', mediapath: '%s' }) "
            "ON CREATE SET m.id = %i, m.created_at = timestamp(), "
            "m.feed_ids = ['%s'] "
            "ON MATCH SET m.feed_ids = m.feed_ids + ['%s']") % \
            (mediatype.title(), node_handle, mediapath, u_id, feed_id, feed_id)

def _add_media_node_to_graph(node_handle, mediatype, mediapath, feed_id, q):
    if node_handle is None:
        node_handle = _filename_from_path(mediapath)
    u_id = _get_unique_id(node_handle)
    q.append(
        _get_create_media_query(
            node_handle, u_id, mediatype, mediapath, feed_id
        )
    )
    return u_id


def _add_node_to_graph(node_handle, node_idx, json_feed, q):
    u_id = 0
    feed_id = json_feed['_id']
    if (node_handle[0] != '$'):
        u_id = _get_unique_id(node_handle)
        q.append(_get_create_concept_query(node_handle, u_id, feed_id))
    else:
        node_handle = node_handle[1:] # strip dollar sign
        mediatype, mediapath = _get_media_type_path(node_idx, json_feed)
        u_id = _add_media_node_to_graph(
            node_handle, mediatype, mediapath, feed_id, q)
    return u_id

def _add_edge_to_graph(edge_name, from_node_id, to_node_id, edge_props, feed_id, q):
    # get rid of '' around keys since cypher doesn't like that
    props_str = re.sub("'(\\w+)':", r'\1:', str(edge_props))
    q.append((
        "MATCH (from { id: %i }), (to { id: %i }) "
        "MERGE (from)-[r:%s %s ]->(to) "
        "ON CREATE SET r.created_at = timestamp(), r.feed_ids = ['%s'] "
        "ON MATCH SET r.feed_ids = r.feed_ids + ['%s']") % \
        (from_node_id, to_node_id, edge_name.upper(), props_str, feed_id, feed_id)
    )

def add_feed_to_graph(json_feed):
    """ Public method supported in the api to add a json feed to the graph. """

    q = [] # queries to execute
    handles = [_normalize_handle_name(m.group(1)) \
                for m in re.finditer('#([^ .]+)', json_feed['text'])]
    # Add nodes to graph
    handle_graph_ids = []
    for i, handle in enumerate(handles):
        handle_graph_ids.append(_add_node_to_graph(handle, i, json_feed, q))

    edges_with_indices = _get_edges_with_handle_indices(json_feed)
    edge_props = _get_edge_props(json_feed)
    feed_id = json_feed['_id']

    # Add edges to graph
    for edge in edges_with_indices:
        from_node_id = handle_graph_ids[edge[1]]
        to_node_id = handle_graph_ids[edge[2]]
        _add_edge_to_graph(
            edge[0], from_node_id, to_node_id, edge_props, feed_id, q)

    # Add remaining media nodes and edges to graph
    media_handle_set = set(
        [str(i) for i, handle in enumerate(handles) if handle[0] == '$'])
    for i, media_relation in enumerate(json_feed['mediamap']):
        related_nodes_set = set(re.findall(r'#(\d+)', media_relation))
        if len(media_handle_set & related_nodes_set) == 0: # #$ already handled
            media_node_id = _add_media_node_to_graph(
                    None, json_feed['mediatype'][i], json_feed['media'][i], 
                    feed_id, q)
            for node_idx in related_nodes_set:
                _add_edge_to_graph('has_media', handle_graph_ids[int(node_idx)], 
                                    media_node_id, edge_props, feed_id, q)

    for query in q:
        tx.append(query)
    tx.commit()
    