

import json
import time

from concurrent import futures
from hamcrest import \
    assert_that, \
    has_key, \
    has_length, \
    is_, \
    is_not, \
    only_contains

from quimby.util.matchers import is_accepted
from quimby.util.test import DbPerTest

import quimby.data as data


class ChangesAPITest(DbPerTest):

    def test_basic(self):
        self.db.doc_save({"_id": "Rob", "location": "Bristol"}, w=3)

        r = self.poll_changes()

        assert_that(r.status_code, is_accepted)
        assert_that(
            r.json().keys(),
            only_contains("results", "last_seq", "pending")
        )
        assert_that(r.json()["results"], has_length(1))

        last_seq = r.json()["last_seq"]

        self.db.doc_save({"_id": "Simon", "location": "Bristol"}, w=3)

        r = self.poll_changes(params={"since": last_seq})

        assert_that(r.json()["results"], has_length(1))

        new_docid = r.json()["results"][0]["id"]
        assert_that(new_docid, is_("Simon"))

    def test_longpoll(self):
        self.db.doc_save({"_id": "Rob", "location": "Bristol"}, w=3)

        with self.res.return_errors():
            p = {"feed": "longpoll"}
            r = self.res.get(self.db.path("_changes"), params=p)

        assert_that(r.status_code, is_accepted)
        assert_that(
            r.json().keys(),
            only_contains("results", "last_seq", "pending")
        )
        assert_that(r.json()["results"], has_length(1))

        last_seq = r.json()["last_seq"]

        self.db.doc_save({"_id": "Simon", "location": "Bristol"})

        with self.res.return_errors():
            p = {"feed": "longpoll", "since": last_seq}
            r = self.res.get(self.db.path("_changes"), params=p)

        assert_that(r.json()["results"], has_length(1))
        new_docid = r.json()["results"][0]["id"]
        assert_that(new_docid, is_("Simon"))

    def test_continuous(self):
        self.db.doc_save({"_id": "Rob", "location": "Bristol"}, w=3)

        with self.res.return_errors():
            p = {"feed": "continuous", "heartbeat": "500"}
            r = self.res.get(self.db.path("_changes"), params=p, stream=True)

        assert_that(r.status_code, is_accepted)

        change = self.get_next_change(r)
        assert_that(change, is_not(None))
        assert_that(change, only_contains("changes", "id", "seq"))
        assert_that(change["id"], is_("Rob"))

        # Get a heartbeat timeout so we know that we just had
        # the one change to report
        change = self.get_next_change(r)
        assert_that(change, is_(None))

        new_doc = {"_id": "Simon", "location": "Bristol"}
        with futures.ThreadPoolExecutor(max_workers=1) as e:
            e.submit(self.background_update, new_doc)

        change = self.get_next_change(r)
        assert_that(change, is_not(None))
        assert_that(change, only_contains("changes", "id", "seq"))
        assert_that(change["id"], is_("Simon"))

    def test_changes_filter(self):
        self.db.bulk_docs(data.simple_docs(), w=3)

        ddoc = {
            "_id": "_design/filter_test",
            "filters": {
                "all": "function(doc, req) { return true; }",
                "none": "function(doc, req) { return false; }"
            }
        }
        self.db.doc_save(ddoc)

        with self.res.return_errors():
            p = {"filter": "filter_test/all"}
            r = self.res.get(self.db.path("_changes"), params=p)

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_key("results"))
        # +1 for the design document
        assert_that(r.json()["results"], has_length(len(data.simple_docs()) + 1))

        with self.res.return_errors():
            p = {"filter": "filter_test/none"}
            r = self.res.get(self.db.path("_changes"), params=p)

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_key("results"))
        assert_that(r.json()["results"], has_length(0))

    def poll_changes(self, params=None):
        # We may need to run this a few times to wait for
        # data to appear on the feed.
        for i in range(5):
            with self.res.return_errors():
                r = self.res.get(self.db.path("_changes"), params=params)
            if r.status_code >= 400:
                break
            if len(r.json().get("results", [])) != 0:
                break
        return r

    def get_next_change(self, resp):
        """
        Returns a change from the continuous feed. None is
        returned after five heartbeats (ie, a timeout)
        """
        for count, line in enumerate(resp.iter_lines(chunk_size=1)):
            if line.strip():
                return json.loads(line)
            # Another heartbeat
            if count > 5:
                return None

    def background_update(self, doc):
        time.sleep(1)
        return self.db.doc_save(doc, w=3)
