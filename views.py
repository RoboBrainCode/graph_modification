#! /usr/bin/env python

from util import get_edges_with_handles, create_graph_elements, get_media, get_edge_props

def add_feed_to_graph(json_feed):
    edges = get_edges_with_handles(json_feed)
    for edge in edges:
        print edge
        create_graph_elements(edge, get_media(json_feed), get_edge_props(json_feed))
