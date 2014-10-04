#! /usr/bin/env python

from util import get_handles, create_graph_elements, get_media, get_edge_props

def add_feed_to_graph(json_feed):
    handles = get_handles(json_feed)
    for handle in handles:
        print handle
        create_graph_elements(handle, get_media(json_feed), get_edge_props(json_feed))
