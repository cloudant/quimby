
import json

from hamcrest import assert_that, has_entry, has_key
from quimby.util.matchers import is_accepted, is_not_found
from quimby.util.test import DbPerClass


class DocAPITests(DbPerClass):

    def test_nonexistent_document(self):
        with self.res.return_errors():
            r = self.res.get(self.db.path("doc_which_doesnt_exist"))
        assert_that(r.status_code, is_not_found)

    def test_existing_doc(self):
        with self.res.return_errors():
            d = json.dumps({"_id": "Ulises", "location": "Aberdeen"})
            r = self.res.put(self.db.path("Ulises"), data=d)

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_key("id"))
        assert_that(r.json(), has_key("rev"))

        with self.res.return_errors():
            r = self.res.get(self.db.path("Ulises"))

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_key("_id"))
        assert_that(r.json(), has_key("_rev"))
        assert_that(r.json(), has_entry("location", "Aberdeen"))

    def test_create_ddoc(self):
        ddoc = {
            "_id": "_design/foo",
            "views": {
                "bar": {
                    "map": "function(doc) { emit(doc.key, 1); }"
                }
            }
        }
        with self.res.return_errors():
            d = json.dumps(ddoc)
            r = self.res.put(self.db.path("_design/foo"), data=d)

        assert_that(r.status_code, is_accepted)

        with self.res.return_errors():
            r = self.res.get(self.db.path("_design/foo"))

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_entry("_id", "_design/foo"))
        assert_that(r.json(), has_key("_rev"))
        assert_that(r.json(), has_entry("views", ddoc["views"]))
