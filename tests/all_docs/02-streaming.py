

from hamcrest import assert_that, has_key, has_length, only_contains
from quimby.util.test import DbPerClass

import quimby.data as data


NUM_DOCS = 150


class AllDocsStreamingTests(DbPerClass):

    Q = 1

    @classmethod
    def setUpClass(klass):
        self.db.bulk_docs(data.gen_docs(NUM_DOCS), w=3)

    def test_basic(self):
        r = self.db.all_docs()
        assert_that(r.rows, has_length(NUM_DOCS))

    def test_include_docs(self):
        r = self.db.all_docs(include_docs=True)
        assert_that(r.rows, has_length(NUM_DOCS))
        assert_that(r.rows, only_contains(has_key("doc")))
