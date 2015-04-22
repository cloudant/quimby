
import time

from hamcrest import *

import cloudant


NUM_ROWS = 100

# All nodes are node1 with an update_seq of 0
SINCE_SEQ = "".join("""\
    0-g1AAAACjeJzLYWBgYMlgTmHgz8tPSTV0M
    DQy1zMAQsMcoARTIkOS_f___7MSGXAqyWMB
    kgwHgNR_olQ2QFTux6cyyQFIJtVDzMsCAEo
    dK6Y
""".split())


def setup_module():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset(q=4)
    docs = []
    for i in range(NUM_ROWS):
        docs.append({"value":i})
    db.bulk_docs(docs)
    # Wait for internal replication
    time.sleep(2)


def test_basic_node_replacement():
    # First run the changes on node1 so we get the
    # last update seq.
    node1 = cloudant.get_server(node="node1@127.0.0.1", interface="public")
    node1_private = cloudant.get_server(node="node1@127.0.0.1", interface="private")
    db = node1.db("test_suite_db")
    c = db.changes(since=SINCE_SEQ)
    assert_that(c.results, has_length(NUM_ROWS))
    last_seq = c.last_seq

    # Now put node1 into maintenance mode and then
    # run the changes feed again to make sure that
    # we don't get all of the updates again.
    try:
        node1_private.config_set("couchdb", "maintenance_mode", "true")
        node3 = cloudant.get_server(node="node3@127.0.0.1", interface="public")
        db = node3.db("test_suite_db")
        c = db.changes(since=last_seq)
        assert_that(c.results, has_length(0))
    finally:
        node1_private.config_set("couchdb", "maintenance_mode", "false")
