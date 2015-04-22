
from hamcrest import *

import cloudant


NUM_ROWS = 100


def setup_module():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset(q=4)
    docs = []
    for i in range(NUM_ROWS):
        docs.append({"value":i})
    db.bulk_docs(docs)


def test_all_docs():
    # Check that we can run with maintenance mode on a number
    # of servers. This has an assumption that the last node in
    # the cluster has a fully shard ring. To make this more better
    # we should compare the list of nodes returned to the shard
    # map so that we can remove non-shard-containing nodes from
    # service first.
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    nodes = cloudant.nodes(interface="private")
    try:
        for n in nodes[:-1]:
            n.config_set("couchdb", "maintenance_mode", "true")
            v = db.all_docs()
            assert_that(v.rows, has_length(100))
        n = nodes[-1]
        n.config_set("couchdb", "maintenance_mode", "true")
        try:
            db.all_docs()
        except:
            assert_that(srv.res.last_req.json(), has_key("error"))
            assert_that(srv.res.last_req.json(),
                has_entry("reason", "No DB shards could be opened."))
        else:
            raise AssertionError("View should not complete successfully")
    finally:
        for n in nodes:
            n.config_set("couchdb", "maintenance_mode", "false")
