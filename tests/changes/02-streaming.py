
import json
import time

from concurrent import futures
from hamcrest import \
    all_of, \
    assert_that, \
    greater_than, \
    has_entry, \
    has_key, \
    has_length, \
    has_property, \
    is_, \
    is_not, \
    less_than, \
    only_contains

from quimby.util.test import DbPerClass

import quimby.data as data


NUM_DOCS = 150
DOC_COUNT = 151  # Because design doc

# This is a since sequence with a bad node name in it that
# I created in Erlang. I changed one of the node names to
# be something like notreallyanode@127.0.0.1 to guarantee
# that we would get replacement.
BAD_SINCE = "".join("""
    153-g2wAAAABaANkABhub3RyZWFsbHlhbm9kZUAxMjcuMC4wLjFsAAAAAm
    EAbgQA_____2poAmGZbQAAAAdhMjAxMjU0ag
""".split())


class ChangesStreamingTests(DbPerClass):

    @classmethod
    def setUpClass(klass):
        super(ChangesStreamingTests, klass).setUpClass()
        klass.db.bulk_docs(data.gen_docs(NUM_DOCS), w=3)
        klass.db.doc_save(data.simple_map_red_ddoc(), w=3)

    def test_normal(self):
        c = self.db.changes()
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(DOC_COUNT))
        assert_that(c.results, only_contains(has_key("seq")))

    def test_longpoll(self):
        c = self.db.changes(feed="longpoll")
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(DOC_COUNT))
        assert_that(c.results, only_contains(has_key("seq")))

    def test_longpoll_timeout(self):
        last_seq = self.db.changes().last_seq
        before = time.time()
        c = self.db.changes(feed="longpoll", since=last_seq, timeout=1000)
        after = time.time()
        assert_that(c.results, has_length(0))
        assert_that(after - before, greater_than(0.5))

    def test_longpoll_with_update(self):
        c = self.db.changes()
        last_seq = c.last_seq

        # Grab a random doc to update in the background
        docid = c.results[-1]["id"]
        doc = self.db.doc_open(docid)

        def background_update():
            time.sleep(1)
            return self.db.doc_save(doc.copy())

        with futures.ThreadPoolExecutor(max_workers=1) as e:
            f = e.submit(background_update)

        # Start our changes feed listener
        before = time.time()
        c = self.db.changes(feed="longpoll", since=last_seq, timeout=5000)
        after = time.time()

        assert_that(after - before, less_than(4))

        newdoc = f.result()
        assert_that(doc["_id"], is_(newdoc["_id"]))
        assert_that(doc["_rev"], is_not(newdoc["_rev"]))

        assert_that(c.results, has_length(1))
        assert_that(c.results[0], has_entry("id", doc["_id"]))

    def test_continuous(self):
        c = self.db.changes(feed="continuous", timeout=500)
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(DOC_COUNT))
        changes_row = all_of(has_key("seq"), has_key("changes"))
        assert_that(c.results, only_contains(changes_row))

    def test_continuous_with_update(self):
        c = self.db.changes()
        last_seq = c.last_seq
        docid = c.results[-1]["id"]
        doc = self.db.doc_open(docid)

        def background_update():
            time.sleep(1)
            return self.db.doc_save(doc.copy())

        with futures.ThreadPoolExecutor(max_workers=1) as e:
            f = e.submit(background_update)

        before = time.time()
        c = self.db.changes(feed="continuous", since=last_seq, timeout=2000)
        c.read()
        duration = time.time() - before

        newdoc = f.result()
        assert_that(doc["_id"], is_(newdoc["_id"]))
        assert_that(doc["_rev"], is_not(newdoc["_rev"]))

        assert_that(duration, greater_than(2))
        assert_that(c.results, has_length(1))
        assert_that(c.results[0], has_entry("id", doc["_id"]))

    def test_normal_shard_replacement(self):
        c = self.db.changes(since=BAD_SINCE)
        # Since we have a q=1 then each shard will have the
        # total DOC_COUNT docs so a replacement means we
        # get the entire changes sequence again.
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(DOC_COUNT))
        # Our new last_seq should also be valid and give us
        # zero changes
        c = self.db.changes(since=c.last_seq)
        assert_that(c.results, has_length(0))

    def test_longpoll_shard_replacement(self):
        c = self.db.changes(feed="longpoll", since=BAD_SINCE)
        # Since we have a q=1 then each shard will have the
        # total DOC_COUNT docs so a replacement means we
        # get the entire changes sequence again.
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(DOC_COUNT))
        # Our new last_seq should also be valid and give us
        # zero changes
        c = self.db.changes(feed="longpoll", since=c.last_seq, timeout=250)
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(0))

    def test_continuous_shard_replacement(self):
        c = self.db.changes(feed="continuous", since=BAD_SINCE, timeout=500)
        # Since we have a q=1 then each shard will have the
        # total DOC_COUNT docs so a replacement means we
        # get the entire changes sequence again.
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(DOC_COUNT))
        # Our new last_seq should also be valid and give us
        # zero changes
        c = self.db.changes(feed="continuous", since=c.last_seq, timeout=250)
        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(0))

    def test_continuous_with_heartbeat(self):
        r = self.res.get(self.db.path("_changes"), stream=True, params={
            "feed": "continuous",
            "heartbeat": "200"
        })
        changes_read = 0
        heart_beats = 0
        for line in r.iter_lines(chunk_size=2):
            if not line.strip():
                heart_beats += 1
                if heart_beats == 3:
                    break
            else:
                assert_that(json.loads(line), has_key("id"))
                changes_read += 1
        assert_that(changes_read, is_(DOC_COUNT))
        assert_that(heart_beats, is_(3))
        # Forcefully close the underlying socket here so
        # that its not reused because we don't consume
        # the entire response.
        r.raw._fp.close()

    def test_continuous_with_heartbeat_and_since(self):
        last_seq = self.db.changes().last_seq

        r = self.res.get(self.db.path("_changes"), stream=True, params={
            "feed": "continuous",
            "heartbeat": 200,
            "since": last_seq
        })
        changes_read = 0
        heart_beats = 0
        for line in r.iter_lines(chunk_size=2):
            if not line.strip():
                heart_beats += 1
                if heart_beats == 3:
                    break
            else:
                assert_that(json.loads(line), has_key("id"))
                changes_read += 1
        assert_that(changes_read, is_(0))
        assert_that(heart_beats, is_(3))
        # Forcefully close the underlying socket here so
        # that its not reused because we don't consume
        # the entire response.
        r.raw._fp.close()

    def test_changes_with_seq_interval(self):
        seq_interval = 3
        changes_read = 0
        c = self.db.changes(seq_interval=seq_interval)
        for row in c.results:
            changes_read += 1
            if changes_read % seq_interval == 2:
                assert_that(row.get("seq"), is_not(None))
            else:
                assert_that(row, has_entry("seq", None))

        assert_that(c, has_property("last_seq"))
        assert_that(c.results, has_length(DOC_COUNT))
        assert_that(c.results, only_contains(has_key("seq")))
