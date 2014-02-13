
import contextlib as ctx

from hamcrest import *

import cloudant


@ctx.contextmanager
def nodes_up(num):
    nodes = cloudant.nodes(interface="public")
    assert num > 0 and num <= len(nodes)
    try:
        for n in nodes[num:]:
            n.config_set("cloudant", "maintenance_mode", "true")
        yield nodes[0].db("test_suite_db")
    finally:
        for n in nodes[num:]:
            n.config_set("cloudant", "maintenance_mode", "false")


def setup_module():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset(q=1)
    doc = {"_id": "foo"}
    db.doc_save(doc)
    ddoc = {
        "_id": "_design/foo",
        "views": {
            "direct": {
                "map": "function(doc) {emit(null, null);}"
            },
            "linked": {
                "map": "function(doc) {emit(null, {\"_id\":doc._id});}",
            }
        }
    }
    db.doc_save(ddoc)


def test_doc_read_quorum_matrix():
    for num_nodes in range(1, 4):
        with nodes_up(num_nodes) as db:
            for r in range(1, 4):
                if r <= num_nodes:
                    matches = is_not(has_key("_r_met"))
                else:
                    matches = has_entry("_r_met", False)
                doc = db.doc_open("foo", r=r)
                assert_that(doc, matches, "num: %s r: %s" % (num_nodes, r))
            # Also make sure default R works cause paranoia
            if num_nodes < 2: # r=2 is defualt
                matches = has_entry("_r_met", False)
            else:
                matches = is_not(has_key("_r_met"))
            doc = db.doc_open("foo")
            assert_that(doc, matches)


def test_all_docs():
    # _all_docs?include_docs=true is an implicit R=1 read
    # since we read from local shards without a cluster
    # call.
    for num_nodes in range(1, 4):
        with nodes_up(num_nodes) as db:
            all_docs = db.all_docs(include_docs=True)
            docs = [r["doc"] for r in all_docs.rows]
            assert_that(docs, only_contains(is_not(has_key("_r_met"))))


def test_all_docs_post_keys():
    # If we POST a keys object to _all_docs?include_docs=true
    # then we are making a clustered lookup which results
    # in quorum information being passed along.
    for num_nodes in range(1, 4):
        with nodes_up(num_nodes) as db:
            all_docs = db.all_docs(include_docs=True, keys=["foo"])
            docs = [r["doc"] for r in all_docs.rows]
            if num_nodes < 2:
                assert_that(docs, only_contains(has_entry("_r_met", False)))
            else:
                assert_that(docs, only_contains(is_not(has_key("_r_met"))))


def test_views():
    # By default views with include_docs=true will do
    # an R=1 read from the shard servicing the request
    for num_nodes in range(1, 4):
        with nodes_up(num_nodes) as db:
            view = db.view("foo", "direct", include_docs=True)
            docs = [r["doc"] for r in view.rows]
            assert_that(docs, only_contains(is_not(has_key("_r_met"))))


def test_views_post_keys():
    # Unlike _all_docs, POSTing keys to a view does not
    # switch to a clustered lookup because we can't
    # know which shard a key is on a priori.
    for num_nodes in range(1, 4):
        with nodes_up(num_nodes) as db:
            view = db.view("foo", "direct", include_docs=True)
            docs = [r["doc"] for r in view.rows]
            assert_that(docs, only_contains(is_not(has_key("_r_met"))))


def test_views_linked_docs():
    # Views have the ability to include docs based on the
    # value emitted for the row. Since the docid emitted may
    # live on a different shard this is turned into a cluster
    # lookup.
    # N.B. For the test we can emit the docid of the doc being
    # indexed and still trigger the clustered lookup because we
    # don't have the optimization to check the hash of the linked
    # doc id.
    for num_nodes in range(1, 4):
        with nodes_up(num_nodes) as db:
            view = db.view("foo", "linked", include_docs=True)
            docs = [r["doc"] for r in view.rows]
            if num_nodes < 2:
                assert_that(docs, only_contains(has_entry("_r_met", False)))
            else:
                assert_that(docs, only_contains(is_not(has_key("_r_met"))))
