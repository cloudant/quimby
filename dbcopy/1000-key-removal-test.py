import json
import time

import cloudant

from hamcrest import *


DDOC = {
    "_id": "_design/bar",
    "views": {
        "bam": {
            "map": "function(doc) {emit(null, 1);}",
            "reduce": "_sum",
            "dbcopy": "test_suite_db_key_removal_test_dbcopy"
        }
    }
}


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")
def test_removal():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db_key_removal_test")
    dbcopy = srv.db("test_suite_db_key_removal_test_dbcopy")

    db.reset()
    dbcopy.reset()

    db.doc_save(DDOC)

    # Show that adding rows to a view udpates dbcopy correctly

    doc1 = db.doc_save({}, w=3)

    v = db.view("bar", "bam", group=True)
    assert_that(v.rows, has_length(1))
    assert_that(v.rows[0], has_entry("value", 1))

    db.wait_for_indexers(design_doc="bar")

    v = dbcopy.all_docs(include_docs=True)
    assert_that(v.rows, has_length(1))
    assert_that(v.rows[0], has_key("doc"))
    assert_that(v.rows[0]["doc"], has_entry("value", 1))

    doc2 = db.doc_save({}, w=3)

    v = db.view("bar", "bam", group=True)
    assert_that(v.rows, has_length(1))
    assert_that(v.rows[0], has_entry("value", 2))

    db.wait_for_indexers(design_doc="bar")

    v = dbcopy.all_docs(include_docs=True)
    assert_that(v.rows, has_length(1))
    assert_that(v.rows[0], has_key("doc"))
    assert_that(v.rows[0]["doc"], has_entry("value", 2))

    # Make sure that deleting one of the source documents
    # updates the view and dbcopy doc appropriately
    db.doc_delete(doc1, w=3)

    v = db.view("bar", "bam", group=True)
    assert_that(v.rows, has_length(1))
    assert_that(v.rows[0], has_entry("value", 1))

    db.wait_for_indexers(design_doc="bar")

    v = dbcopy.all_docs(include_docs=True)
    assert_that(v.rows, has_length(1))
    assert_that(v.rows[0], has_key("doc"))
    assert_that(v.rows[0]["doc"], has_entry("value", 1))

    # Make sure removing the last doc from the source view
    # results in an empty view and dbcopy db
    db.doc_delete(doc2, w=3)

    v = db.view("bar", "bam", group=True)
    assert_that(v.rows, has_length(0))

    db.wait_for_indexers(design_doc="bar")

    v = dbcopy.all_docs()
    assert_that(v.rows, has_length(0))
