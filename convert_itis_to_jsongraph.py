#!/usr/bin/env python
# --coding:utf-8--

import os
import sqlite3
import jsonstreams
import datetime
import hashlib
import json


def convert_itis(itis: sqlite3.Connection, itis_md5: str, graph: jsonstreams.Object):
    #   "$schema": "http://json-schema.org/draft-07/schema#",
    #   "$id": "http://jsongraphformat.info/v2.1/json-graph-schema.json",
    write_graph_attributes(graph, itis_md5)

    with graph.subobject('nodes') as nodes:
        write_kingdom_nodes(itis, nodes)
        write_rank_nodes(itis, nodes)

    with graph.subarray('edges') as edges:
        write_rank_edges(itis, edges)


def __kingdom_label(kingdom_id: int) -> str:
    return f'kingdom-{kingdom_id}'


def write_kingdom_nodes(itis: sqlite3.Connection, nodes: jsonstreams.Object):
    kingdoms = itis.execute('''
        SELECT kingdom_id, kingdom_name, update_date
        FROM kingdoms 
        ORDER BY kingdom_id
        ''')

    for (kingdom_id, kingdom_name, update_date) in kingdoms:
        with nodes.subobject(__kingdom_label(kingdom_id)) as kingdom:
            kingdom.write('label', kingdom_name)
            with kingdom.subobject('metadata') as meta:
                meta.write('type', 'kingdom')
                meta.write('itis_kingdom_id', kingdom_id)
                meta.write('update_date', update_date)


def __rank_label(kingdom_id: int, rank_id: int) -> str:
    return f'rank-{kingdom_id}.{rank_id}'


def write_rank_nodes(itis: sqlite3.Connection, nodes: jsonstreams.Object):
    # Fetch all ranks from the Plantae kingdom (kingdom_id = 3)
    taxon_unit_types = itis.execute('''
        SELECT kingdom_id, rank_id, rank_name, update_date
        FROM taxon_unit_types 
        WHERE kingdom_id = 3 ORDER BY rank_id
        ''')

    for (kingdom_id, rank_id, rank_name, update_date) in taxon_unit_types:
        with nodes.subobject(__rank_label(kingdom_id, rank_id)) as rank:
            rank.write('label', rank_name)
            with rank.subobject('metadata') as meta:
                meta.write('type', 'taxon_unit_type')
                meta.write('itis_rank_id', rank_id)
                meta.write('update_date', update_date)


def write_rank_edges(itis: sqlite3.Connection, edges: jsonstreams.Array):
    # Fetch all ranks from the Plantae kingdom (kingdom_id = 3)
    taxon_unit_types = itis.execute('''
        SELECT kingdom_id, rank_id, dir_parent_rank_id, req_parent_rank_id
        FROM taxon_unit_types 
        WHERE kingdom_id = 3 ORDER BY rank_id
        ''')

    for (kingdom_id, rank_id, dir_parent_rank_id, req_parent_rank_id) in taxon_unit_types:
        with edges.subobject() as edge:
            edge.write('source', __kingdom_label(kingdom_id))
            edge.write('target', __rank_label(kingdom_id, rank_id))
            edge.write('relation', 'uses')

        if dir_parent_rank_id != rank_id:
            with edges.subobject() as edge:
                edge.write('source', __rank_label(kingdom_id, dir_parent_rank_id))
                edge.write('target', __rank_label(kingdom_id, rank_id))
                edge.write('relation', 'direct_parent_of')

        if req_parent_rank_id != rank_id:
            with edges.subobject() as edge:
                edge.write('source', __rank_label(kingdom_id, req_parent_rank_id))
                edge.write('target', __rank_label(kingdom_id, rank_id))
                edge.write('relation', 'required_parent_of')


def write_graph_attributes(graph, itis_md5):
    graph.write('id', 'itis-042721')
    graph.write('type', 'ITIS')
    graph.write('label', 'ITIS (2021-04-27)')
    graph.write('directed', True)
    write_graph_metadata(graph, itis_md5)


def write_graph_metadata(graph, itis_md5):
    with graph.subobject('metadata') as graph_meta:
        graph_meta.write('created', datetime.datetime.now().isoformat())
        with graph_meta.subobject('metadata') as source:
            source.write('type', 'sqlite')
            source.write('md5', itis_md5)


def __md5(fname: str) -> str:
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def convert_to_dot(input_file: str, output_file: str):
    with open(input_file, 'r', encoding='utf-8') as f:
        j = json.load(f)

    graph = j['graph']
    with open(output_file, 'w', encoding='ascii') as f:
        f.write(f'digraph "{graph["id"]}" {{\n')
        nodes = graph['nodes']
        for key in nodes:
            node = nodes[key]
            f.write(f'"{key}" [label="{node["label"]}"];\n')

        edges = graph['edges']
        for edge in edges:
            f.write(f'"{edge["source"]}" -> "{edge["target"]}" [label="{edge["relation"]}"];\n')

        f.write('}')


def main():
    INPUT_DB = 'ITIS-042721.sqlite'
    OUTPUT_JSON = 'ITIS-042721.json'
    OUTPUT_DOT = 'ITIS-042721.dot'

    input_path = os.path.join('data', INPUT_DB)
    md5 = __md5(input_path)

    itis = sqlite3.connect(input_path)
    output_path = os.path.join('data', OUTPUT_JSON)

    with jsonstreams.Stream(jsonstreams.Type.OBJECT, filename=output_path, indent=2, pretty=True) as s:
        with s.subobject('graph') as graph:
            convert_itis(itis, md5, graph)

    convert_to_dot(output_path, os.path.join('data', OUTPUT_DOT))


if __name__ == '__main__':
    main()
