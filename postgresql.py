#!/usr/bin/env python3
##: Date: 2020-10-09 - 2020-10-09
##: Author: Tomas Andriekus @ <t.andriekus at gmail dot com>
##: Requirements: > Python3.6 & psycopg module
##: Description: PostgreSQL stats script rewritten from Ivan Dimitrov Perl script. Big Thanks to him //
## Originally dedicated for Observium - Network management and monitoring tool //
## Should work with Postgres > 9 and above. //
## https://docs.observium.org/apps/#postgresql //

import psycopg2

# Please *SECURE your credentials*
db_host='127.0.0.1'
db_user='postgres'
db_pass='whatsthes3cr3t'
db_name='mydb'

PG_ACTIVITY='''
    SELECT datname, usename, client_addr, query FROM pg_stat_activity;
    '''
NUM_COMMITS='''
    SELECT SUM(xact_commit) as xact_commit, SUM(xact_rollback) as xact_rollback, SUM(blks_read) as blks_read,
    SUM(blks_hit) as blks_hit, SUM(tup_returned) as tup_returned, SUM(tup_fetched) as tup_fetched,
    SUM(tup_inserted) as tup_inserted, SUM(tup_updated) as tup_updated, SUM(tup_deleted) as tup_deleted
    FROM pg_stat_database;
    '''

version = []
pg_commits = {}
pg_connection_count = {'count' : 0}
pg_datnames = []
pg_usenames = []
pg_client_addrs = []
pg_idle = []
pg_select = []
pg_update = []
pg_delete = []
pg_other = []

def analyze_pg_version(pg_ver):
    if int(pg_ver) < 100000:
        return str(pg_ver)[:1] + '.' + str(pg_ver)[1:2]
    elif int(pg_ver) > 100000 and int(pg_ver) < 1000000:
        return str(pg_ver)[:2] + '.' + str(pg_ver)[2:3]
    else:
        # This should never happen.
        return("???")

def execute_pg_statement(cursor, statement):
    cursor.execute(statement)
    data = cursor.fetchall()
    return data

def aggregate_connections_data(data):
    count = len(data)

    if str(count) != '0' or count != None:
        cc = 0
        for i in data:
            seen_datnames = set(pg_datnames)
            seen_usenames = set(pg_usenames)
            seen_client_addrs = set(pg_client_addrs)

            if i[0] not in seen_datnames and i[0] != None and i[0] != '':
                pg_datnames.append(i [0])
            if i[1] not in seen_usenames and i[1] != None and i[0] != '':
                pg_usenames.append(i[1])
            if i[2] != None:
                cc += 1
                pg_connection_count['count'] = cc

                if i[2] not in seen_client_addrs and i[2] != None:
                    pg_client_addrs.append(i[2])

            if i[3] != None or i[3] != '':
                if 'SELECT' or 'select' in i[3]:
                    pg_select.append(i[3])
                elif 'UPDATE' or 'update' in i[3]:
                    pg_update.append(i [3])
                elif 'DELETE' or 'delete' in i[3]:
                    pg_delete.append(i [3])
                elif '<IDLE>' in i[3]:
                    pg_idle.append(i[3])
                else:
                    pg_other.append(i[3])

def aggregate_commits(data):
    # This enum correlates with - NUM_COMMITS query;
    pg_commits['xact_commit'] = (data[0])
    pg_commits['xact_rollback'] = (data[1])
    pg_commits ['blks_read'] = (data [2])
    pg_commits ['blks_hit'] = (data [3])
    pg_commits ['tup_returned'] = (data [4])
    pg_commits ['tup_fetched'] = (data [5])
    pg_commits ['tup_inserted'] = (data [6])
    pg_commits ['tup_updated'] = (data [7])
    pg_commits ['tup_deleted'] = (data [8])

def fetch():
    try:
        conn = psycopg2.connect(host=db_host, user=db_user, password=db_pass, database=db_name)

        version.append(analyze_pg_version(conn.server_version))

        cursor = conn.cursor()

        # Execute SQL queries;
        pg_activity_result = execute_pg_statement(cursor, PG_ACTIVITY)
        aggregate_connections_data(pg_activity_result)

        pg_num_activity_result = execute_pg_statement(cursor, NUM_COMMITS)
        aggregate_commits(pg_num_activity_result[0])

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        conn.close()


if __name__ == '__main__':
    fetch()

print(f"<<<app-postgresql>>>")
print(f"version:{version[0]}")
print(f"cCount:{pg_connection_count.get('count')}")
print(f"tDbs:{len(pg_datnames)}")
print(f"tUsr:{len(pg_usenames)}")
print(f"tHst:{len(pg_client_addrs)}")
print(f"idle:{len(pg_idle)}")
print(f"select:{len(pg_select)}")
print(f"update:{len(pg_update)}")
print(f"delete:{len(pg_delete)}")
print(f"other:{len(pg_other)}")
print(f"xact_commit:{pg_commits.get('xact_commit')}")
print(f"xact_rollback:{pg_commits.get('xact_rollback')}")
print(f"blks_read:{pg_commits.get('blks_read')}")
print(f"blks_hit:{pg_commits.get('blks_hit')}")
print(f"tup_returned:{pg_commits.get('tup_returned')}")
print(f"tup_fetched:{pg_commits.get('tup_fetched')}")
print(f"tup_inserted:{pg_commits.get('tup_inserted')}")
print(f"tup_updated:{pg_commits.get('tup_updated')}")
print(f"tup_deleted:{pg_commits.get('tup_deleted')}")
