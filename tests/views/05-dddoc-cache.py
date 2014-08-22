
import json
import textwrap

from hamcrest import assert_that, has_entry
from quimby.util.matchers import is_forbidden
from quimby.util.test import DbPerClass


class ViewDDocCacheTests(DbPerClass):

    Q = 8

    @classmethod
    def setUpClass(klass):
        super(ViewDDocCacheTests, klass).setUpClass()
        docs = [
            {"_id": "a", "key": "a", "val": 1},
            {"_id": "b", "key": "b", "val": 1},
            {"_id": "c", "key": "c", "val": 1},
            {"_id": "d", "key": "a", "val": 2},
            {"_id": "e", "key": "b", "val": 3},
            {"_id": "f", "key": "f", "val": 4},
            {"_id": "g", "key": "a", "val": 0},
            {"_id": "h", "key": "b", "val": 0},
            {"_id": "i", "key": "c", "val": 0},
            {"_id": "j", "key": "a", "val": 0},
            {"_id": "k", "key": "b", "val": 0},
            {"_id": "l", "key": "f", "val": 0},
            {"_id": "m", "key": "a", "val": 0},
            {"_id": "n", "key": "b", "val": 0},
            {"_id": "o", "key": "c", "val": 0},
            {"_id": "p", "key": "a", "val": 0},
            {"_id": "q", "key": "b", "val": 0},
            {"_id": "r", "key": "f", "val": 0}
        ]
        klass.db.bulk_docs(docs)

    def test_ddoc_one(self):
        design = self.save_design(self.ddoc_one())
        self.check_one()
        self.delete_design(design)

    def test_ddoc_two(self):
        design = self.save_design(self.ddoc_two())
        self.check_two()
        self.delete_design(design)

    def test_successive_updates(self):
        design = self.save_design(self.ddoc_one())
        design = self.save_design(self.ddoc_two(), design)
        self.check_two()
        design = self.save_design(self.ddoc_two(), design)
        design = self.save_design(self.ddoc_one(), design)
        self.check_one()
        self.delete_design(design)

    def test_coherent_validation_funs(self):
        self.db.doc_save({
            "_id": "s",
            "key": "x",
            "val": 0,
            "reject_me": True
        })
        design = self.save_design(self.ddoc_one())
        self.db.doc_save({
            "_id": "t",
            "key": "x",
            "val": 0,
            "reject_me": True
        })
        design = self.save_design(self.ddoc_two(), design)
        with self.res.return_errors():
            body = json.dumps({
                "_id": "u",
                "key": "x",
                "val": 0,
                "reject_me": True
            })
            r = self.res.put(self.db.path("u"), data=body)
        assert_that(r.status_code, is_forbidden)
        design = self.save_design(self.ddoc_one(), design)
        self.db.doc_save({
            "_id": "v",
            "key": "x",
            "val": 0,
            "reject_me": True
        })
        self.delete_design(design)

    def check_one(self):
        view = self.db.view("counting", "values")
        assert_that(view.rows[0], has_entry("value", 12))

    def check_two(self):
        view = self.db.view("counting", "values")
        assert_that(view.rows[0], has_entry("value", 9))

    def save_design(self, doc, prev=None):
        if prev is not None:
            doc["_rev"] = prev["_rev"]
        self.db.doc_save(doc, w=3)
        return doc

    def delete_design(self, doc):
        self.db.doc_delete(doc, w=3)

    def ddoc_one(self):
        value_map = textwrap.dedent("""\
            function(doc) {
                emit(doc.key, doc.val);
            }""")
        return {
            "_id": "_design/counting",
            "views": {
                "values": {
                    "map": value_map,
                    "reduce": "_sum"
                }
            }
        }

    def ddoc_two(self):
        kv_map = textwrap.dedent("""\
            function(doc) {
                if (doc.val > 1) emit(doc.key, doc.val);
            }""")
        validation = textwrap.dedent("""\
            function(doc, prev, ctx) {
                if (doc.reject_me) {
                    throw({forbidden: "denied because we need to test this"});
                }
            }""")
        return {
            "_id": "_design/counting",
            "views": {
                "values": {
                    "map": kv_map,
                    "reduce": "_sum"
                }
            },
            "validate_doc_update": validation
        }
