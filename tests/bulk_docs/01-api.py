

from hamcrest import assert_that, is_
from quimby.util.test import DbPerTest

import quimby.data as data


class BulkDocsAPITest(DbPerTest):

    def test_bulk_docs(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        ulises = self.db.doc_open("Ulises")
        ulises["location"] = "Bristol"

        bob = self.db.doc_open("Bob")
        bob["location"] = "Bristol"

        self.db.bulk_docs([ulises, bob], w=3)

        new_ulises = self.db.doc_open("Ulises")
        assert_that(new_ulises["location"], is_("Bristol"))

        new_bob = self.db.doc_open("Bob")
        assert_that(new_bob["location"], is_("Bristol"))
