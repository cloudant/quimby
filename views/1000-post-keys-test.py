
import textwrap

from hamcrest import *

import cloudant


DB = None


def setup_module():
    global DB
    srv = cloudant.get_server()
    DB = srv.db("test_suite_db")
    DB.reset(q=8)
    docs = [
        {"_id": "a", "key": "a", "vals": 1},
        {"_id": "b", "key": "b", "vals": 1},
        {"_id": "c", "key": "c", "vals": 3},
        {"_id": "d", "key": "a", "vals": 1},
        {"_id": "e", "key": 1, "vals": 2},
        {"_id": "f", "key": "f", "vals": 1}
    ]
    DB.bulk_docs(docs)
    DB.doc_save(make_ddoc()) # make_ddoc down below


def test_empty_keys():
    _exec([], [])


def test_missing_key():
    _exec(["z"], [])


def test_single_unique_key():
    _exec(["b"], ["b"])


def test_one_key_present_one_key_missing():
    _exec(["b", "z"], ["b"])


def test_one_key_present_one_key_missing_different_order():
    _exec(["z", "b"], ["b"])


def test_multiple_keys():
    _exec(["b", "f"], ["b", "f"])


def test_multiple_keys_different_order():
    _exec(["f", "b"], ["f", "b"])


def test_repeated_keys():
    _exec(["b", "b"], ["b", "b"])


def test_single_key_multiple_rows():
    _exec(["a"], [("a", "a"), ("d", "a")])


def test_repeated_key_with_multiple_rows():
    # This needs to change after BugzId: 23155
    _exec(["a", "a"], [("a", "a"), ("a", "a"), ("d", "a"), ("d", "a")])


def test_bad_fabric_error():
    # This needs to change after BugzId: 23155
    keys = ["a", "b", "a"]
    expect = ["b", ("a", "a"), ("a", "a"), ("d", "a"), ("d", "a")]
    _exec(keys, expect)


def test_multiple_multiple_rows():
    # This needs to change after BugzId: 23155
    keys = ["a", "a", "c"]
    expect = [("a", "a"), ("a", "a"), ("d", "a"), ("d", "a"), "c", "c", "c"]
    _exec(keys, expect)


def test_multiples_with_repeated():
    keys = ["a", "c", "b", "b"]
    expect = [("a", "a"), ("d", "a"), "c", "c", "c", "b", "b"]
    _exec(keys, expect)


def _exec(keys, expect):
    v = DB.view("foo", "bar", keys=keys)
    assert_that(v.rows, has_length(len(expect)))
    for i, k in enumerate(expect):
        if isinstance(k, tuple):
            docid, key = k
        else:
            docid, key = (k, k)
        assert_that(v.rows[i], has_entry("id", docid))
        assert_that(v.rows[i], has_entry("key", key))


def make_ddoc():
    bar_map = textwrap.dedent("""\
    function(doc) {
        if(!doc.key) {
            emit(doc._id, null);
            return;
        }
        for(var i = 0; i < doc.vals; i++) {
            emit(doc.key, null);
        }
    }""")
    bam_map = textwrap.dedent("""\
    function(doc) {
        if(!doc.key) {
            emit(doc._id, 0);
            return;
        }
        for(var i = 0; i < doc.vals; i++) {
            emit([doc.key], 1);
            emit([doc.key, true], 1);
        }
    }
    """)
    bam_red = "_sum"
    return {
        "_id": "_design/foo",
        "views": {
            "bar": {
                "map": bar_map
            },
            "bam": {
                "map": bam_map,
                "reduce": bam_red
            }
        }
    }

