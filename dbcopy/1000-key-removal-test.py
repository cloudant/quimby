import json
import time

import cloudant
import t


DDOC = {
    "_id": "_design/bar",
    "views": {
        "bam": {
            "map": "function(doc) {emit(null, 1);}",
            "reduce": "_sum",
            "dbcopy": "test_suite_db_copy"
        }
    }
}


def test_removal():
    db = cloudant.default_server().db("test_suite_db")
    dbcopy = cloudant.default_server().db("test_suite_db_copy")

    db.reset()
    dbcopy.reset()

    db.doc_save(DDOC)

    doc1 = db.doc_save({})
    v = db.view("bar", "bam", group=True)
    t.eq(len(v.rows), 1, "There should be a single row in the view")
    t.eq(v.rows[0]["value"], 1, "The row should have a value of 1")
    
    db.wait_for_indexers(design_doc="bar")

    v = copydb.all_docs(include_docs=True)
    t.eq(len(v.rows), 1, "There should be a single doc in the dbcopy db")
    t.eq(v.rows[0]["doc"]["value"], 1, "The dbcopy value should be 1")

    doc2 = db.doc_save({})
    view = db.view("bar", "bam", group=True)
    t.eq(len(v.rows), 1, "There should be a single row in the view")
    t.eq(v.rows[0]["value"], 2, "The single row should have a new value")

    db.wait_for_indexers(design_doc="bar")

    view = copydb.all_docs(include_docs=True)
    t.eq(len(v.rows), 1, "There should still only be a single dbcopy doc")
    t.eq(v.rows["doc"]["value"], 2, "The dbcopy doc should have a new value")

    # Make sure that deleting one of the source documents
    # updates the view and dbcopy doc appropriately
    db.delete(doc1)

    v = db.view("bar", "bam", group=True)
    t.eq(len(v.rows), 1, "There should be a single view row")
    t.eq(v.rows[0]["value"], 1, "The value should be 1 again")

    db.wait_for_indexers(design_doc="bar")
    
    v = copydb.all_docs(include_docs=True)
    t.eq(len(v.rows), 1, "There should be a single doc in the dbcopy db")
    t.eq(v.rows[0]["doc"]["value"], 1, "The dbcopy doc value should be 1")

    # Make sure removing the last doc from the source view
    # results in an empty view and dbcopy db
    db.delete(doc2)

    v = db.view("bar", "bam", group=True)
    t.eq(len(v.rows), 0, "There should be no more rows in the view")

    db.wait_for_indexers(design_doc="bar")

    v = copydb.all_docs()
    t.eq(len(v.rows), 0, "There should be no more docs in the dbcopy db")
