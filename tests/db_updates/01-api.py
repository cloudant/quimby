
import json
import random
import time
import uuid

from concurrent import futures
from hamcrest import \
    anything, \
    assert_that, \
    greater_than, \
    greater_than_or_equal_to, \
    has_entries, \
    has_entry, \
    has_item, \
    has_key, \
    has_length, \
    has_property, \
    is_, \
    is_not, \
    less_than, \
    less_than_or_equal_to, \
    only_contains

from quimby.util.matchers import is_bad_request
from quimby.util.test import DbPerTest


class GlobalChangesAPITests(DbPerTest):

    Q = 1

    def setUp(self):
        super(GlobalChangesAPITests, self).setUp()
        db = self.srv.db("global_changes")
        if not db.exists():
            db.create()

    def test_no_options(self):
        # Smoke
        c = self.srv.global_changes()
        assert_that(c, has_property("results"))
        assert_that(c, has_property("last_seq"))

    def test_db_event_types(self):
        # I'm mashing each of these tests together because cycling
        # through dbs is a good way to get the dbcore nodes to hit
        # the file descriptor limit which leads to terribleness.

        def get_changes(self, seq):
            kwargs = {
                "feed": "continuous",
                "since": seq,
                "timeout": 500,
                "limit": 10
            }
            return self.srv.global_changes(**kwargs)

        seq = self.srv.global_changes().last_seq

        dbname = "test_global_changes_" + uuid.uuid4().hex
        db = self.srv.db(dbname)
        db.create(q=1)

        c = get_changes(seq)
        pattern = {"dbname": dbname, "type": "created"}
        assert_that(c.results, has_item(has_entries(pattern)))

        seq = c.last_seq

        db.doc_save({"foo": "bar"}, w=3)
        c = get_changes(seq)
        pattern = {"dbname": dbname, "type": "updated"}
        assert_that(c.results, has_item(has_entries(pattern)))

        seq = c.last_seq

        # Multiple db updatse gives a single row with a different
        # update sequence
        for i in range(25):
            db.doc_save({"ohai": random.randint(0, 500)}, w=3)

        c = get_changes(seq)
        pattern = {"dbname": dbname, "type": "updated", "seq": is_not(seq)}
        assert_that(c.results, has_item(has_entries(pattern)))

        seq = c.last_seq

        db.delete()
        c = get_changes(seq)
        pattern = {"dbname": dbname, "type": "deleted"}
        assert_that(c.results, has_item(has_entries(pattern)))

    def test_limit(self):
        db = self.srv.db("global_changes")

        # Make sure we have more than the required number
        # of events in the database so this test works. Might
        # need to add some events if this ever fails.
        assert_that(db.info()["doc_count"], greater_than_or_equal_to(4))

        c1 = self.srv.global_changes(limit=2)
        assert_that(c1.results, has_length(2))

        # Check that limit with a since value returns new events
        c2 = self.srv.global_changes(limit=2, since=c1.last_seq)
        for row in c2.results:
            assert_that(c1.results, is_not(has_item(row)))

    def test_parameters_are_ignored(self):
        c = self.srv.global_changes(limit=1, foo="bar")
        assert_that(c.results, has_length(1))
        assert_that(c, has_property("last_seq"))

    def test_400_on_bad_paramters(self):
        tests = [
            ("feed", None),
            ("feed", False),
            ("feed", 1.0),
            ("since", None),
            ("since", False),
            ("since", 3.14),
            ("limit", None),
            ("limit", -1),
            ("limit", False),
            ("heartbeat", None),
            ("heartbeat", "bam"),
            ("timeout", None),
            ("timeout", -10),
            ("timeout", "ohai")
        ]
        with self.res.return_errors():
            for (k, v) in tests:
                if not isinstance(v, basestring):
                    v = json.dumps(v)
                r = self.res.get("_db_updates", params={k: v})
                assert_that(r.status_code, is_bad_request)

    def test_descending(self):
        db = self.srv.db("global_changes")

        # Make sure we have enough events in the database
        # for this test to succeed.
        assert_that(db.info()["doc_count"], greater_than_or_equal_to(4))

        c = self.srv.global_changes(limit=5, descending=True)

        def int_seq(res):
            if isinstance(res["seq"], list):
                return res["seq"][0]
            else:
                return int(res["seq"].split("-", 1)[0])
        start = int_seq(c.results[0])

        # There's a bug in the underlying changes behavior that does weird
        # things to the since sequence on descending requests. We should
        # figure that out at somepoint.
        for row in c.results[1:]:
            assert_that(int_seq(row), less_than_or_equal_to(start))

    def test_long_poll(self):
        c = self.srv.global_changes(feed="longpoll")
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, only_contains(has_key("seq")))

    def test_long_poll_limit(self):
        c = self.srv.global_changes(feed="longpoll", limit=3)
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(3))
        assert_that(c.results, only_contains(has_key("seq")))

    def test_long_poll_timeout(self):
        # This is a bit racey in that any activity in the system
        # may cause us to return before the timeout. We'll just
        # retry a couple times and hope that we're idle enough that
        # we can hit a timeout.
        num_tries = 10
        for i in range(num_tries):
            seq = self.srv.global_changes().last_seq

            before = time.time()
            c = self.srv.global_changes(
                feed="longpoll",
                since=seq,
                timeout=500
            )
            after = time.time()

            if after - before > 0.45:
                break

        # -1 because of the behavior of range
        assert_that(i, less_than(num_tries - 1))
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(0))

    def long_poll_with_update(self):
        seq = self.srv.global_changes().last_seq

        def background_update():
            time.sleep(1)
            return self.db.doc_save({})

        with futures.ThreadPoolExecutor(max_workers=1) as e:
            f = e.submit(background_update)

        before = time.time()
        c = self.srv.global_changes(feed="longpoll", since=seq, timeout=5000)
        after = time.time()

        assert_that(after - before, less_than(4))

        newdoc = f.result()
        pattern = {"_id": anything(), "_rev": anything()}
        assert_that(newdoc, has_entries(pattern))
        assert_that(c.entries, has_item(has_entry("dbname", self.db.name)))

    def test_continuous(self):
        c = self.srv.global_changes(feed="continuous", timeout=500)
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, only_contains(has_key("seq")))

    def test_continuous_limit(self):
        c = self.srv.global_changes(feed="continuous", limit=4, timeout=500)
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(4))
        assert_that(c.results, only_contains(has_key("seq")))

    def continuous_with_update(self):
        seq = self.srv.global_changes().last_seq

        def background_update():
            time.sleep(1)
            return self.db.doc_save({})

        with futures.ThreadPoolExecutor(max_workers=1) as e:
            f = e.submit(background_update)

        before = time.time()
        c = self.srv.global_changes(feed="continuous", since=seq, timeout=2000)
        c.read()
        after = time.time()

        newdoc = f.result()
        pattern = {"_id": anything(), "_rev": anything()}
        assert_that(newdoc, has_entries(pattern))
        assert_that(after - before, greater_than(2))
        assert_that(c.results, has_item(has_entry("dbname", self.db.name)))

    def test_continuous_heartbeat(self):
        r = self.res.get("_db_updates", stream=True, params={
            "feed": "continuous",
            "heartbeat": "200"
        })
        changes_read = 0
        heart_beats = 0
        for line in r.iter_lines(chunk_size=2):
            if not line.strip():
                heart_beats += 1
                if heart_beats >= 3:
                    break
            else:
                assert_that(json.loads(line), has_key("dbname"))
                changes_read += 1
        assert_that(changes_read, greater_than(0))
        assert_that(heart_beats, is_(3))
        r.close()
