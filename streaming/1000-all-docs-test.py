
from hamcrest import *

import cloudant
import streaming.util


def test_all_docs():
    db = streaming.util.create_streaming_db()
    v = db.all_docs()
    assert_that(v.rows, has_length(streaming.util.NUM_DOCS))
    assert_that(v.rows, has_item(has_entry("id", "_design/foo")))


def test_all_docs_include_docs():
    db = streaming.util.create_streaming_db()
    v = db.all_docs(include_docs=True)
    assert_that(v.rows, has_length(streaming.util.NUM_DOCS))
    assert_that(v.rows, only_contains(has_key("doc")))
