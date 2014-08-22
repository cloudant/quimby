
import copy

from hamcrest import \
    any_of, \
    assert_that, \
    has_entry, \
    has_key, \
    has_length, \
    instance_of, \
    is_, \
    only_contains

from quimby.util.test import DbPerClass

import quimby.data as data


NUM_DOCS = 200
INT_TYPE = any_of(instance_of(int), instance_of(long))


class StreamingMapViewTests(DbPerClass):

    @classmethod
    def setUpClass(klass):
        self.db.bulk_docs(data.gen_docs(NUM_DOCS, value=1), w=3)
        self.db.doc_save(copy.deepcopy(data.SIMPLE_MAP_RED_DDOC), w=3)

    def test_map(self):
        v = self.db.view("foo", "bar")
        assert_that(v.total_rows, is_(NUM_DOCS))
        assert_that(v.rows, has_length(NUM_DOCS))
        assert_that(v.rows, only_contains(has_entry("value", INT_TYPE)))

    def test_map_stale_ok(self):
        v = self.db.view("foo", "bar", stale="ok")
        assert_that(v.total_rows, is_(NUM_DOCS))
        assert_that(v.rows, has_length(NUM_DOCS))
        assert_that(v.rows, only_contains(has_entry("value", INT_TYPE)))

    def test_map_include_docs(self):
        v = self.db.view("foo", "bar", include_docs=True)
        assert_that(v.total_rows, is_(NUM_DOCS))
        assert_that(v.rows, has_length(NUM_DOCS))
        assert_that(v.rows, only_contains(has_key("doc")))

    def test_reduce(self):
        v = self.db.view("foo", "bam")
        assert_that(v.rows, has_length(1))
        assert_that(v.rows[0], has_entry("value", NUM_DOCS))

    def test_reduce_group_true(self):
        v = self.db.view("foo", "bam", group=True)
        assert_that(v.rows, has_length(2))
        assert_that(v.rows, only_contains(is_(NUM_DOCS / 2)))

    def test_reduce_stale_ok(self):
        v = self.db.view("foo", "bam", stale="ok")
        assert_that(v.rows, has_length(1))
        assert_that(v.rows[0], has_entry("value", NUM_DOCS))
