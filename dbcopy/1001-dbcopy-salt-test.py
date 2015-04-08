#! /usr/bin/env python

import cloudant

from hamcrest import *

DDOC = {
    "_id": "_design/foo",
    "views": {
        "bar": {
            "map": "function(doc) {emit(doc._id, 1);}",
            "reduce": "_sum",
            "dbcopy": "test_suite_db_copy"
        }
    }
}


def test_doc_salt():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    dbcopy = srv.db("test_suite_db_copy")

    db.reset()
    dbcopy.reset()

    # Insert a doc and get it into the dbcopy db
    db.doc_save(DDOC)
    doc1 = db.doc_save({"_id":"test"}, w=3)
    db.view("foo", "bar", group=True)
    db.wait_for_indexers(design_doc="foo")

    # Check that we have the expected dbcopy doc
    copydocid = cloudant.dbcopy_docid("test")
    copydoc1 = dbcopy.doc_open(copydocid)
    assert_that(copydoc1, has_entry("key", "test"))
    assert_that(copydoc1, has_entry("value", 1))

    # Remove the source view row
    db.doc_delete(doc1)
    db.view("foo", "bar", group=True)
    db.wait_for_indexers(design_doc="foo")

    # Check that the dbcopy doc is gone
    with db.srv.res.return_errors() as res:
        r = res.get(dbcopy.path(copydocid))
        assert_that(r.status_code, is_(404))

    # Get the shard map for the dbcopy db
    dbsdb = cloudant.random_node().db("dbs")
    smap = dbsdb.doc_open(dbcopy.name)
    suffix = "".join(chr(c) for c in smap["shard_suffix"])

    # Force compaction on all shards in the dbcopy
    # database
    for name in smap["by_node"]:
        node = cloudant.get_server(node=name, interface="private")
        for rng in smap["by_node"][name]:
            args = (rng, dbcopy.name, suffix)
            shard = node.db("shards%%2F%s%%2F%s%s" % args)
            shard.compact(wait=True)

    # Readding row to source view and dbcopy db
    doc1.pop("_rev")
    db.doc_save(doc1)
    db.view("foo", "bar", group=True)
    db.wait_for_indexers(design_doc="foo")

    # Check that our dbcopy doc is back
    copydoc2 = dbcopy.doc_open(copydocid)
    assert_that(copydoc2["_rev"], is_not(copydoc1["_rev"]))
    assert_that(copydoc2, has_entry("key", "test"))
    assert_that(copydoc2, has_entry("value", 1))
    assert_that(copydoc2, has_key("salt"))
