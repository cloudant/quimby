import cloudant
import textwrap
import json
import uuid

from hamcrest import *


DB = None


def setup_module():
    global DB
    srv = cloudant.get_server()
    DB = srv.db("test_suite_db")
    DB.reset(q=32)
    docs = [
        {"_id": "a", "key": "a", "val": 1},
        {"_id": "b", "key": "b", "val": 1},
        {"_id": "c", "key": "c", "val": 1},
        {"_id": "d", "key": "a", "val": 2},
        {"_id": "e", "key": "b", "val": 3},
        {"_id": "f", "key": "f", "val": 4},
        {"_id": "g", "key": "a", "val": 0},
        {"_id": "h", "key": "b", "val": 0},
        {"_id": "i", "key": "c", "val": 0},
        {"_id": "j", "key": "a", "val": 0},
        {"_id": "k", "key": "b", "val": 0},
        {"_id": "l", "key": "f", "val": 0},
        {"_id": "m", "key": "a", "val": 0},
        {"_id": "n", "key": "b", "val": 0},
        {"_id": "o", "key": "c", "val": 0},
        {"_id": "p", "key": "a", "val": 0},
        {"_id": "q", "key": "b", "val": 0},
        {"_id": "r", "key": "f", "val": 0}]
    DB.bulk_docs(docs)


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")
def test_ddoc_one():
    design = save_design(ddoc_one)
    check_one()
    delete_design(design)


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")
def test_ddoc_two():
    design = save_design(ddoc_two)
    check_two()
    delete_design(design)


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")
def test_successive_updates():
    design = save_design(ddoc_one)
    design = save_design(ddoc_two, design)
    check_two()
    design = save_design(ddoc_two, design)
    design = save_design(ddoc_one, design)
    check_one()
    delete_design(design)


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")
def test_coherent_validation_funs():
    DB.doc_save({"_id": "s", "key": "x", "val": 0, "reject_me": True})
    design = save_design(ddoc_one)
    DB.doc_save({"_id": "t", "key": "x", "val": 0, "reject_me": True})
    design = save_design(ddoc_two, design)
    with DB.srv.res.return_errors() as res:
        body = json.dumps({"_id": "u", "key": "x", "val": 0, "reject_me": True})
        res.put(DB.path("u"), data = body)
        assert_that(res.last_req.status_code, is_(403))
    design = save_design(ddoc_one, design)
    DB.doc_save({"_id": "v", "key": "x", "val": 0, "reject_me": True})
    delete_design(design)


def check_one():
    view = DB.view("counting", "values")
    assert_that(view.rows[0], has_entry("value", 12))


def check_two():
    view = DB.view("counting", "values")
    assert_that(view.rows[0], has_entry("value", 9))


def save_design(generator, prev = None):
    doc = generator()
    if prev != None:
        doc["_rev"] = prev["_rev"]
    DB.doc_save(doc, w = 3)
    return doc


def delete_design(doc):
    DB.doc_delete(doc, w = 3)


def ddoc_one():
    value_map = textwrap.dedent("""\
        function(doc) {
            emit(doc.key, doc.val);
        }""")
    return {
        "_id": "_design/counting",
        "views": {
            "values": {
                "map": value_map,
                "reduce": "_sum"}}}


def ddoc_two():
    kv_map = textwrap.dedent("""\
        function(doc) {
            if (doc.val > 1) emit(doc.key, doc.val);
        }""")
    validation = textwrap.dedent("""\
        function(doc, prev, ctx) {
            if (doc.reject_me) {
                throw({forbidden: "denied because we need to test this"});
            }
        }""")
    return {
        "_id": "_design/counting",
        "views": {
            "values": {
                "map": kv_map,
                "reduce": "_sum"}},
        "validate_doc_update": validation}
