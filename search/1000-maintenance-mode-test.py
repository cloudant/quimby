
import time

from hamcrest import *

import cloudant


NUM_ROWS = 100

DDOC = {
    "_id": "_design/searchtest",
    "indexes": {
        "searchtest": {
            "index": "function(doc) {if(doc.value){index(\"value\", doc.value);}}"
        }
    }
}


def setup_module():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset(q=1)
    db.doc_save(DDOC)
    docs = []
    for i in range(NUM_ROWS):
        docs.append({"value":i})
    db.bulk_docs(docs)


def test_search():
    run_search()

def run_search():
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
    nodes = cloudant.nodes(interface="private")
    try:
        for n in nodes[:-1]:
            n.config_set("cloudant", "maintenance_mode", "true")
            s = db.search("searchtest", "searchtest", query="*:*", stale="ok")
            assert_that(s["total_rows"], greater_than_or_equal_to(0))
        n = nodes[-1]
        n.config_set("cloudant", "maintenance_mode", "true")
        try:
            db.search("searchtest", "searchtest", query="*:*", stale="ok")
        except:
            assert_that(srv.res.last_req.json(), has_key("error"))
            assert_that(srv.res.last_req.json(),
                has_entry("reason", "No DB shards could be opened."))
        else:
            raise AssertionError("Search Results should not be returned")
    finally:
        for n in nodes:
            n.config_set("cloudant", "maintenance_mode", "false")
