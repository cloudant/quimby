
from hamcrest import *

import cloudant
import streaming.util

int_type = any_of(instance_of(int), instance_of(long))

def test_map_view():
    db = streaming.util.create_streaming_db()
    v = db.view("foo", "bar")
    assert_that(v.rows, has_length(streaming.util.NUM_MAP_ROWS))
    assert_that(v.rows,
        only_contains(has_entry("value", int_type)))


def test_map_view_stale_ok():
    db = streaming.util.create_streaming_db()
    v = db.view("foo", "bar", stale="ok")
    assert_that(v.rows,
        only_contains(has_entry("value", int_type)))


def test_map_view_include_docs():
    db = streaming.util.create_streaming_db()
    v = db.view("foo", "bar", include_docs=True)
    assert_that(v.rows, only_contains(has_key("doc")))
