
from hamcrest import *

import cloudant
import streaming.util


def test_red_view():
    db = streaming.util.create_streaming_db()
    v = db.view("foo", "bam")
    assert_that(v.rows, has_length(streaming.util.NUM_RED_ROWS[0]))
    assert_that(v.rows[0], has_entry("value", 1000))


def test_red_view_group_true():
    db = streaming.util.create_streaming_db()
    v = db.view("foo", "bam", group=True)
    # Technically we could have random numbers collide
    # but it shouldn't be very many
    assert_that(streaming.util.NUM_RED_ROWS[2] - len(v.rows), less_than(3))


def test_red_view_group_level():
    db = streaming.util.create_streaming_db()
    v = db.view("foo", "bam", group_level=1)
    # Technically we're relying on a uniform random
    # distribution so. Make sure we cover at least
    # seven of the eight possibilities
    assert_that(streaming.util.NUM_RED_ROWS[1] - len(v.rows), less_than(2))


def test_red_view_stale_ok():
    db = streaming.util.create_streaming_db()
    v = db.view("foo", "bam", stale="ok")
    assert_that(v.rows, has_length(streaming.util.NUM_RED_ROWS[0]))
    assert_that(v.rows[0], has_entry("value", 1000))
