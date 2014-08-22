
import json

from hamcrest import \
    assert_that, \
    greater_than, \
    has_entry, \
    has_items, \
    has_key, \
    has_length, \
    is_, \
    only_contains

from quimby.util.matchers import is_accepted
from quimby.util.test import DbPerTest

import quimby.data as data


class AllDocsAPITest(DbPerTest):
    Q = 1

    def test_get_empty(self):
        with self.res.return_errors():
            r = self.res.get(self.db.path("/_all_docs"))

        assert_that(r.status_code, is_accepted)
        assert_that(r.json().keys(), has_items("total_rows", "rows", "offset"))
        assert_that(r.json()["total_rows"], is_(0))
        assert_that(r.json()["offset"], is_(0))
        assert_that(r.json()["rows"], has_length(0))

    def test_ascending(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        with self.res.return_errors():
            r = self.res.get(self.db.path("/_all_docs"))

        assert_that(r.status_code, is_accepted)

        found = [row["key"] for row in r.json()["rows"]]
        expected = sorted(doc["_id"] for doc in data.simple_docs())
        assert_that(found, is_(expected))

    def test_descending(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        with self.res.return_errors():
            p = {"descending": "true"}
            r = self.res.get(self.db.path("_all_docs"), params=p)

        assert_that(r.status_code, is_accepted)

        found = [row["key"] for row in r.json()["rows"]]
        expected = sorted(
            (doc["_id"] for doc in data.simple_docs()),
            reverse=True)
        assert_that(found, is_(expected))

    def test_startkey(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        with self.res.return_errors():
            p = {"startkey": json.dumps("Mike")}
            r = self.res.get(self.db.path("_all_docs"), params=p)

        assert_that(r.status_code, is_accepted)

        found = [row["key"] for row in r.json()["rows"]]
        assert_that(found, has_length(greater_than(0)))
        assert_that(found[0], is_("Mike"))

    def test_endkey(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        with self.res.return_errors():
            p = {"endkey": json.dumps("Mike")}
            r = self.res.get(self.db.path("_all_docs"), params=p)

        assert_that(r.status_code, is_accepted)

        found = [row["key"] for row in r.json()["rows"]]
        assert_that(found, has_length(greater_than(0)))
        assert_that(found[-1], is_("Mike"))

    def test_inclusive_end(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        with self.res.return_errors():
            p = {
                "endkey": json.dumps("Mike"),
                "inclusive_end": "false"
            }
            r = self.res.get(self.db.path("_all_docs"), params=p)

        assert_that(r.status_code, is_accepted)

        found = [row["key"] for row in r.json()["rows"]]
        assert_that(found, has_length(greater_than(0)))
        assert_that(found[-1], is_("Bob"))

    def test_include_docs(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        with self.res.return_errors():
            p = {"include_docs": "true"}
            r = self.res.get(self.db.path("_all_docs"), params=p)

        assert_that(r.status_code, is_accepted)
        assert_that(r.json()["rows"], only_contains(has_key("doc")))

    def test_key(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        with self.res.return_errors():
            p = {"key": json.dumps("Rob")}
            r = self.res.get(self.db.path("_all_docs"), params=p)

        assert_that(r.status_code, is_accepted)
        assert_that(r.json()["rows"], has_length(1))
        assert_that(r.json()["rows"][0], has_entry("key", "Rob"))

    def test_limit(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        keys = [r["key"] for r in self.db.all_docs().rows]

        for i in range(len(keys)):
            with self.res.return_errors():
                p = {"limit": str(i)}
                r = self.res.get(self.db.path("_all_docs"), params=p)

            assert_that(r.status_code, is_accepted)
            assert_that(r.json()["rows"], has_length(i))

            found = [row["key"] for row in r.json()["rows"]]
            expected = keys[:i]
            assert_that(found, is_(expected))

    def test_skip(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        keys = [r["key"] for r in self.db.all_docs().rows]

        for i in range(len(data.simple_docs())):
            with self.res.return_errors():
                p = {"skip": str(i)}
                r = self.res.get(self.db.path("_all_docs"), params=p)

            assert_that(r.status_code, is_accepted)
            assert_that(r.json()["rows"], has_length(len(keys) - i))

            found = [row["key"] for row in r.json()["rows"]]
            expected = [k for k in keys[i:]]
            assert_that(found, is_(expected))

    def test_post(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        keys = ["Ulises", "Bob"]

        with self.res.return_errors():
            d = json.dumps({"keys": keys})
            r = self.res.post(self.db.path("_all_docs"), data=d)

        assert_that(r.status_code, is_accepted)
        assert_that(r.json()["rows"], has_length(2))

        found = [row["key"] for row in r.json()["rows"]]
        assert_that(found, is_(keys))
