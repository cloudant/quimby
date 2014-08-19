

from hamcrest import *
from quimby.util.matchers import *
from quimby.util.test import *

import quimby.data as data


class BulkDocsAPITest(unittest.TestCase):

    @setup_random_db()
    def setUp(self):
        pass

    def test_bulk_docs(self):
        self.db.bulk_docs(data.SIMPLE_DOCS)

        ulises = self.db.doc_open("Ulises")
        ulises["location"] = "Bristol"

        bob = self.db.doc_open("Bob")
        bob["location"] = "Bristol"

        self.db.bulk_docs([ulises, bob])

        new_ulises = self.db.doc_open("Ulises")
        assert_that(new_ulises["location"], is_("Bristol"))

        new_bob = self.db.doc_open("Bob")
        assert_that(new_bob["location"], is_("Bristol"))

        
