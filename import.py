#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
import time
import sqlite3
from tqdm import tqdm

from nebula2.gclient.net import ConnectionPool, Session
from nebula2.Config import Config


def execute_assert(session: Session, statement: str):
    resp = session.execute(statement)
    assert resp.is_succeeded(), resp.error_msg() + f' on statement: {statement}'


def create_space(session: Session):
    execute_assert(session, 'CREATE SPACE IF NOT EXISTS itis (vid_type = FIXED_STRING(20)); USE itis;')
    execute_assert(session, 'CREATE TAG IF NOT EXISTS rank(itis_rank_id int, name string, updated date);')
    execute_assert(session, 'CREATE TAG INDEX IF NOT EXISTS rank_id_0 ON rank(itis_rank_id);')

    execute_assert(session, 'CREATE EDGE IF NOT EXISTS direct_parent_of();')
    execute_assert(session, 'CREATE EDGE IF NOT EXISTS required_parent_of();')
    execute_assert(session, 'CREATE EDGE IF NOT EXISTS has_rank();')
    execute_assert(session, 'CREATE EDGE IF NOT EXISTS parent_of();')

    # for rank in ["Kingdom", "Subkingdom", "Infrakingdom",
    #              "Superdivision", "Division", "Subdivision", "Infradivision",
    #              "Superclass", "Class", "Subclass", "Infraclass",
    #              "Superorder", "Order", "Suborder",
    #              "Family", "Subfamily",
    #              "Tribe", "Subtribe",
    #              "Genus", "Subgenus",
    #              "Section", "Subsection",
    #              "Species", "Subspecies",
    #              "Variety", "Subvariety",
    #              "Form", "Subform"]:
    #     execute_assert(session, f'CREATE TAG IF NOT EXISTS Rank{rank}(itis_rank_id int);')

    execute_assert(session, 'CREATE TAG IF NOT EXISTS taxonomic_unit('
                            'tsn int, name string, accepted bool,'
                            'created timestamp, updated date,'
                            'unit_ind1 string, unit_name1 string,'
                            'unit_ind2 string, unit_name2 string,'
                            'unit_ind3 string, unit_name3 string,'
                            'unit_ind4 string, unit_name4 string'
                            ');')
    execute_assert(session, 'CREATE TAG INDEX IF NOT EXISTS taxonomic_unit_tsn_0 ON taxonomic_unit(tsn);')

    # We need to wait until Nebula's CREATE operations are carried out.
    # TODO: Replace with a lookup-wait loop
    time.sleep(6)


def create_ranks(session: Session, itis: sqlite3.Connection):
    # Fetch all ranks from the Plantae kingdom (kingdom_id = 3)
    taxon_unit_types = itis.execute('''
                SELECT rank_id, rank_name, dir_parent_rank_id, req_parent_rank_id, update_date
                FROM taxon_unit_types 
                WHERE kingdom_id = 3 ORDER BY rank_id
                ''')

    ranks = []
    direct_parents = []
    required_parents = []
    for (rank_id, rank_name, dir_parent_rank_id, req_parent_rank_id, update_date) in taxon_unit_types:
        ranks.append(f'"rank-{rank_id}":({rank_id}, "{rank_name}", date("{update_date}"))')
        # Skip the Kingdom link on itself
        if rank_id == dir_parent_rank_id or rank_id == req_parent_rank_id:
            continue
        direct_parents.append(f'"rank-{dir_parent_rank_id}"->"rank-{rank_id}":()')
        required_parents.append(f'"rank-{req_parent_rank_id}"->"rank-{rank_id}":()')

    execute_assert(session, f'INSERT VERTEX rank(itis_rank_id, name, updated) VALUES {",".join(ranks)};')
    execute_assert(session, f'INSERT EDGE direct_parent_of() VALUES {",".join(direct_parents)};')
    execute_assert(session, f'INSERT EDGE required_parent_of() VALUES {",".join(required_parents)};')


def wrap_none(input, stringify: bool = False):
    if input is None:
        return "null"
    return f'"{input}"' if stringify else input


def create_taxonomic_units(session: Session, itis: sqlite3.Connection):
    count = itis.execute('SELECT COUNT(*) FROM taxonomic_units WHERE kingdom_id = 3').fetchone()[0]

    units = itis.execute('''
                    SELECT rank_id, tsn,
                           complete_name, name_usage, 
                           unit_ind1, unit_name1,
                           unit_ind2, unit_name2,
                           unit_ind3, unit_name3,
                           unit_ind4, unit_name4,
                           initial_time_stamp, update_date
                    FROM taxonomic_units 
                    WHERE kingdom_id = 3''')
    for unit in tqdm(units, desc='Units', total=count):
        rank_id = wrap_none(unit[0])
        tsn = wrap_none(unit[1])
        complete_name = wrap_none(unit[2])
        name_usage = wrap_none(unit[3])
        unit_ind1 = wrap_none(unit[4], True)
        unit_name1 = wrap_none(unit[5], True)
        unit_ind2 = wrap_none(unit[6], True)
        unit_name2 = wrap_none(unit[7], True)
        unit_ind3 = wrap_none(unit[8], True)
        unit_name3 = wrap_none(unit[9], True)
        unit_ind4 = wrap_none(unit[10], True)
        unit_name4 = wrap_none(unit[11], True)
        initial_time_stamp = wrap_none(unit[12])
        update_date = wrap_none(unit[13])

        initial_time_stamp = initial_time_stamp.replace(' ', 'T')
        is_accepted = 'true' if name_usage == 'accepted' else 'false'

        execute_assert(session, f'''INSERT VERTEX taxonomic_unit(
                tsn, name, accepted, created, updated,
                unit_ind1, unit_name1, unit_ind2, unit_name2,
                unit_ind3, unit_name3, unit_ind4, unit_name4)
            VALUES
                "tsn-{tsn}":(
                {tsn}, "{complete_name}", {is_accepted},
                timestamp("{initial_time_stamp}"), date("{update_date}"),
                {unit_ind1}, {unit_name1},
                {unit_ind2}, {unit_name2},
                {unit_ind3}, {unit_name3},
                {unit_ind4}, {unit_name4}
                );
            ''')
        execute_assert(session, f'INSERT EDGE has_rank() VALUES "tsn-{tsn}"->"rank-{rank_id}":();')

    units = itis.execute('SELECT tsn, parent_tsn FROM taxonomic_units WHERE kingdom_id = 3')
    for unit in tqdm(units, desc='Unit Relationships', total=count):
        tsn = unit[0]
        parent_tsn = unit[1]
        if parent_tsn is not None and parent_tsn > 0:
            execute_assert(session, f'INSERT EDGE parent_of() VALUES "tsn-{parent_tsn}"->"tsn-{tsn}":();')


def import_from_itis(session: Session, itis: sqlite3.Connection):
    create_space(session)
    create_ranks(session, itis)
    create_taxonomic_units(session, itis)


def main():
    client = None
    try:
        config = Config()
        config.max_connection_pool_size = 2

        connection_pool = ConnectionPool()
        assert connection_pool.init([('127.0.0.1', 3699)], config)

        client = connection_pool.get_session('user', 'password')
        assert client is not None

        itis = sqlite3.connect(os.path.join('data', 'ITIS-042721.sqlite'))

        import_from_itis(client, itis)

    except Exception:
        import traceback
        print(traceback.format_exc())
        if client is not None:
            client.release()
        exit(1)


if __name__ == '__main__':
    main()
