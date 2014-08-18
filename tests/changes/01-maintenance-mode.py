
import time

from hamcrest import *

import cloudant


NUM_ROWS = 100

# All nodes are node1 with an update_seq of 0
SINCE_SEQ_1 = "".join("""\
    0-g1AAAACjeJzLYWBgYMlgTmHgz8tPSTV0M
    DQy1zMAQsMcoARTIkOS_f___7MSGXAqyWMB
    kgwHgNR_olQ2QFTux6cyyQFIJtVDzMsCAEo
    dK6Y
""".split())

# All nodes are node3 with update_seq of 400
SINCE_SEQ_2 = "".join("""
    2000-g1AAAACveJzLYWBgYMlgTmHgz8tPST
    V2MDQy1zMAQsMcoARTIkOS_f___7OSGBgYv
    -BUlccCJBkOAKn_xCpugCjeT0BxkgOQTKqH
    m5oFAJG7L4Y
""".split())


def setup_module():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset(q=4)
    docs = []
    for i in range(NUM_ROWS):
        docs.append({"value":i})
    db.bulk_docs(docs)
    # Wait for internal replication to avoid
    # the race.
    time.sleep(2)


def test_basic():
    run_changes(100)


def test_basic_since():
    run_changes(100, since=SINCE_SEQ_1)
    run_changes(0, since=SINCE_SEQ_2)


def test_longpoll():
    run_changes(100, feed="longpoll", timeout=500)


def test_longpoll_since():
    run_changes(100, feed="longpoll", timeout=500, since=SINCE_SEQ_1)
    run_changes(0, feed="longpoll", timeout=500, since=SINCE_SEQ_2)


def test_continuous():
    run_changes(100, feed="continuous", timeout=500)


def test_continuous_since():
    run_changes(100, feed="continuous", timeout=500, since=SINCE_SEQ_1)
    run_changes(0, feed="continuous", timeout=500, since=SINCE_SEQ_2)


def run_changes(num_results, **kwargs):
    # Check that we can run with maintenance mode on a number
    # of servers. This has an assumption that the last node in
    # the cluster has a fully shard ring. To make this more better
    # we should compare the list of nodes returned to the shard
    # map so that we can remove non-shard-containing nodes from
    # service first.
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    nodes = cloudant.nodes(interface="public")
    try:
        for n in nodes[:-1]:
            n.config_set("cloudant", "maintenance_mode", "true")
            try:
                c = db.changes(**kwargs)
            except:
                print srv.res.last_req.text
                raise
            assert_that(c.results, has_length(num_results))
        n = nodes[-1]
        n.config_set("cloudant", "maintenance_mode", "true")
        try:
            c = db.changes(**kwargs)
        except:
            assert_that(srv.res.last_req.json(), has_entry("error", "nodedown"))
        else:
            raise AssertionError("Changes should not complete successfully")
    finally:
        for n in nodes:
            n.config_set("cloudant", "maintenance_mode", "false")
