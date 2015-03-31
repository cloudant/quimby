
import time

from hamcrest import *

import cloudant


NUM_ROWS = 100

DDOC = {
    "_id": "_design/test",
    "views": {
        "test": {
            "map": "function(doc) {emit(doc._id, 1);}",
            "reduce": "_sum"
        }
    }
}


def setup_module():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset(q=4)
    db.doc_save(DDOC)
    docs = []
    for i in range(NUM_ROWS):
        docs.append({"value":i})
    db.bulk_docs(docs)


def test_map_views():
    run_view(False)


def test_reduce_view():
    run_view(True);


def run_view(do_reduce):
    """\
    Check that we can run with maintenance mode on a number
    of servers. This has an assumption that the last node in
    the cluster has a fully shard ring. To make this more better
    we should compare the list of nodes returned to the shard
    map so that we can remove non-shard-containing nodes from
    service first.
    """
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    nodes = cloudant.nodes(interface="public")
    try:
        for n in nodes[:-1]:
            n.config_set("couchdb", "maintenance_mode", "true")
            v = db.view("test", "test", reduce=do_reduce, stale="ok")
            assert_that(v.rows, has_length(greater_than_or_equal_to(0)))
        n = nodes[-1]
        n.config_set("couchdb", "maintenance_mode", "true")
        try:
            db.view("test", "test", reduce=do_reduce, stale="ok")
        except:
            assert_that(srv.res.last_req.json(), has_entry("error", "nodedown"))
        else:
            raise AssertionError("View should not complete successfully")
    finally:
        for n in nodes:
            n.config_set("couchdb", "maintenance_mode", "false")
