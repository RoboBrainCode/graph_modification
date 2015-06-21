#! /usr/bin/env python
import json
# from weaverQuery import *
import sys
import re
import ntpath
import datetime as dt
import hashlib
import ConfigParser
import os
import spellcheck
import datetime
from bson.objectid import ObjectId


import json
import yaml
import urllib,requests
import ast
def PostWeaverQuery(fnName,params):
    query=dict(fnName=fnName,params=params)
    myport=3232
    data=json.dumps(query)
    myURL = "http://127.0.0.1:%s/weaverWrapper/execFn/" % (myport)
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.get(myURL, data=data,headers=headers)
    response=yaml.safe_load(r.text)
    return response



# from threading import Lock
# insertLock=Lock()

globalResult=True

CONFIG = ConfigParser.ConfigParser()
CONFIG.read(os.path.dirname(os.path.abspath(__file__)) + '/config.ini')
NormalizationSection = 'HandleNameNormalizations'

def _normalize_handle_name(handle):
    if handle == None or len(handle) == 0: return handle
    normHandle = handle
    if CONFIG.getboolean(NormalizationSection, 'spellcheck'):
        normHandle = spellcheck.correct(normHandle)
    normHandle = re.sub("[\'\"]", "", normHandle).strip().lower()
    return normHandle.encode('utf8')

def _filename_from_path(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def _get_timestamp():
    epoch = dt.datetime.utcfromtimestamp(0)
    return int((dt.datetime.now() - epoch).total_seconds() * 1000)

def _get_unique_id(node_handle):
    return node_handle
    m = hashlib.md5(node_handle.encode())
    return int(str(int(m.hexdigest(), 16))[0:16])

def _get_edges_with_handle_indices(json_feed):
    """ Returns array of tuples of the form (edge_name, 1, 2) where
    1 and 2 correspond to node/hashtag/handle indices in json_feed['text'].
    """
    edges_with_indices = [];
    ListRelation=json_feed['graphStructure'].split(',')
    for relation in ListRelation:
        relation = relation.strip()
        edge_name_removed = re.sub('.*:','',relation)
        node_indices = \
            [m.group(1) for m in re.finditer(r'#(\d+)', edge_name_removed)]

        assert len(node_indices) == 2
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
    props['keywords']=json_feed['keywords']
    props['source_text'] = str(json_feed['source_text'])
    props['source_url'] = str(json_feed['source_url'])
    return props

def _get_media_type_path(node_idx, json_feed):
    mediatype = ''
    mediapath = ''
    node_idx_str = str(node_idx)
    mediamap=json_feed['mediamap'].split(',')
    mediatype=json_feed['mediatype'].split(',')
    media=json_feed['media'].split(',')
    for i, mediamap_elem in enumerate(mediamap):
        node_indices = set(re.findall(r'#(\d+)', mediamap_elem))

        if node_idx_str in node_indices:
            mediatype = mediatype[i]
            mediapath = media[i]
            break
    return (mediatype, mediapath)


def _add_media_node_to_graph(node_handle, mediatype, mediapath, feed_id,nodeProps):
    if node_handle is None:
        node_handle = _filename_from_path(mediapath)
    u_id = node_handle+'__'+mediapath

    nodeProps['labels']='Media,'+mediatype.title()
    nodeProps['handle']=node_handle
    nodeProps['feed_ids']=feed_id
    nodeProps['mediapath']=mediapath
    global globalResult
    fnName='InsertNode'
    params=dict(Id=u_id,nodeProps=nodeProps)
    result=PostWeaverQuery(fnName,params)

    globalResult=globalResult and result
    
    return u_id


def _add_node_to_graph(node_handle, node_idx, json_feed,nodeProps):
    u_id = 0
    feed_id = json_feed['_id']
    if (node_handle[0] != '$'):
        u_id = _get_unique_id(node_handle)
        nodeProps['handle']=str(node_handle)
        nodeProps['feed_ids']=str(feed_id)
        nodeProps['labels']='Concept'
        global globalResult

        fnName='InsertNode'
        params=dict(Id=str(node_handle),nodeProps=nodeProps)
        result=PostWeaverQuery(fnName,params)

        globalResult=globalResult and  result
    else:
        node_handle = node_handle[1:] # strip dollar sign
        mediatype, mediapath = _get_media_type_path(node_idx, json_feed)
        u_id = _add_media_node_to_graph(
            node_handle, mediatype, mediapath, feed_id,nodeProps)
    return u_id

def _add_edge_to_graph(edge_name, from_node_id, to_node_id, edge_props, feed_id):
    props_str = re.sub("'(\\w+)':", r'\1:', str(edge_props))
    edge_props['label']=edge_name.upper()
    edge_props['feed_ids']=str(feed_id)
    global globalResult

    fnName='InsertRelationUndirected'
    params=dict(src=str(from_node_id),dst=str(to_node_id),edgeProps=edge_props)
    result=PostWeaverQuery(fnName,params)

    globalResult= globalResult and result

def add_weaver_queries(json_feed):
    handles = [_normalize_handle_name(m.group(1)) for m in re.finditer('#([^ .]+)', json_feed['text'])]
    handle_graph_ids = []
    for i, handle in enumerate(handles):
        if json_feed['nodeProps'] and str(i) in json_feed['nodeProps']:
            nodeProps=dict(json_feed['nodeProps'][str(i)])  
	else:
	    nodeProps=dict() 
        handle_graph_ids.append(_add_node_to_graph(handle, i, json_feed,nodeProps))

    edges_with_indices = _get_edges_with_handle_indices(json_feed)
    edge_props = _get_edge_props(json_feed)
    feed_id = json_feed['_id']
    countVal=0
    for edge in edges_with_indices:
        from_node_id = handle_graph_ids[edge[1]]
        to_node_id = handle_graph_ids[edge[2]]
        updatedEdgeProps=dict()
        
        if json_feed['edgeProps'] and str(countVal) in json_feed['edgeProps']:
            updatedEdgeProps=dict(json_feed['edgeProps'][str(countVal)])
        updatedEdgeProps.update(edge_props)
        
        _add_edge_to_graph(
            edge[0], from_node_id, to_node_id, updatedEdgeProps, feed_id)
        countVal=countVal+1
    media_handle_set = set(
        [str(i) for i, handle in enumerate(handles) if handle[0] == '$'])
    

    mediamap=json_feed['mediamap'].split(',')
    media=json_feed['media'].split(',')
    mediatype=json_feed['mediatype'].split(',')

    for i, media_relation in enumerate(mediamap):
        related_nodes_set = set(re.findall(r'#(\d+)', media_relation))

        if len(media_handle_set & related_nodes_set) == 0: # #$ already handled
            media_node_id = _add_media_node_to_graph(
                    None, mediatype[i], media[i], 
                    feed_id,{})
            for node_idx in related_nodes_set:
                _add_edge_to_graph('has_media', handle_graph_ids[int(node_idx)], 
                                    media_node_id, edge_props, feed_id)

def insertInWeaver(response):
    json_feed=dict()
    for key,vals in response.iteritems():
        retVal=""
        if type(vals)==list:
           for val in vals:
               retVal=retVal+','+val
	   retVal=retVal[1:]
        else:
            retVal=vals
        json_feed[key]=retVal
    global globalResult
    globalResult=True
    add_weaver_queries(json_feed)
    return globalResult
