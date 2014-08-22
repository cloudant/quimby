
import copy

from hamcrest import assert_that, has_length, is_
from quimby.util.test import DbPerTest

import quimby.data as data


NUM_DOCS = 25


class ViewAPITests(DbPerTest):

    def setUp(self, *args, **kwargs):
        super(ViewAPITests, self).setUp(*args, **kwargs)
        self.db.bulk_docs(data.gen_docs(NUM_DOCS))
        self.db.doc_save(copy.deepcopy(data.simple_map_red_ddoc()))

    def test_get_view(self):
        v = self.db.view("foo", "bar")
        assert_that(v.total_rows, is_(NUM_DOCS))
        assert_that(v.rows, has_length(NUM_DOCS))

    def test_limit(self):
        v = self.db.view("foo", "bar", limit=1)
        assert_that(v.total_rows, is_(NUM_DOCS))
        assert_that(v.rows, has_length(1))
