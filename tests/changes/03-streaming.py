
import json
import time

from concurrent import futures
from hamcrest import *

import cloudant
import streaming.util

def test_changes():
    db = streaming.util.create_streaming_db()
    c = db.changes()
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(streaming.util.NUM_DOCS))
    assert_that(c.results, only_contains(has_key("seq")))


def test_changes_longpoll():
    db = streaming.util.create_streaming_db()
    c = db.changes(feed="longpoll")
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(streaming.util.NUM_DOCS))
    assert_that(c.results, only_contains(has_key("seq")))


def test_changes_longpoll_timeout():
    db = streaming.util.create_streaming_db()
    last_seq = db.changes().last_seq
    before = time.time()
    c = db.changes(feed="longpoll", since=last_seq, timeout=1000)
    after = time.time()
    assert_that(c.results, has_length(0))
    assert_that(after - before, greater_than(0.5))


def test_changes_longpoll_adding_docs():
    db = streaming.util.create_streaming_db()
    c = db.changes()
    last_seq = c.last_seq

    # Grab a random doc to update in the background
    docid = c.results[-1]["id"]
    doc = db.doc_open(docid)

    def background_update():
        time.sleep(1)
        return db.doc_save(doc.copy())

    with futures.ThreadPoolExecutor(max_workers=1) as e:
        f = e.submit(background_update)

    # Start our changes feed listener
    before = time.time()
    c = db.changes(feed="longpoll", since=last_seq, timeout=5000)
    after = time.time()

    assert_that(after - before, less_than(4))

    newdoc = f.result()
    assert_that(doc["_id"], is_(newdoc["_id"]))
    assert_that(doc["_rev"], is_not(newdoc["_rev"]))

    assert_that(c.results, has_length(1))
    assert_that(c.results[0], has_entry("id", doc["_id"]))


def test_changes_continuous():
    db = streaming.util.create_streaming_db()
    c = db.changes(feed="continuous", timeout=500)
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(streaming.util.NUM_DOCS))
    changes_row = all_of(has_key("seq"), has_key("changes"))
    assert_that(c.results, only_contains(changes_row))


def test_changes_continuous_adding_docs():
    db = streaming.util.create_streaming_db()
    c = db.changes()
    last_seq = c.last_seq
    docid = c.results[-1]["id"]
    doc = db.doc_open(docid)

    def background_update():
        time.sleep(1)
        return db.doc_save(doc.copy())

    with futures.ThreadPoolExecutor(max_workers=1) as e:
        f = e.submit(background_update)

    before = time.time()
    c = db.changes(feed="continuous", since=last_seq, timeout=2000)
    c.read()
    duration = time.time() - before

    newdoc = f.result()
    assert_that(doc["_id"], is_(newdoc["_id"]))
    assert_that(doc["_rev"], is_not(newdoc["_rev"]))

    assert_that(duration, greater_than(2))
    assert_that(c.results, has_length(1))
    assert_that(c.results[0], has_entry("id", doc["_id"]))


# This is a since sequence with a bad node name in it that
# I created in Erlang. I changed one of the node names to
# be something like notreallyanode@127.0.0.1 to guarantee
# that we would get replacement.

BAD_SINCE = "".join("""
    972-g1AAAAFMeJzLYWBg4MhgTmHgz8tPSTV0MDQy1zMAQsMc
    oARTIkOS_P___7MSq3EqyWMBkgwPgNR_sMoaAioPQFTez0ps
    gKs0xqpyAUTl_qzEHAJmNkBUzs9KrMWpMikBSCbVg93YgFuV
    A0hVPFhVGUiVRF5-SVFqYk5OZSJIPbpyBZBye4jyLAARYF5X
""".split())


def test_changes_with_since_replacement():
    db = streaming.util.create_streaming_db()
    c = db.changes(since=BAD_SINCE)
    # The number of docs on each shard is random but should
    # theoretically be more than 50 and less than 800
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(greater_than(50)))
    assert_that(c.results, has_length(less_than(800)))


def test_changes_longpoll_with_since_replacement():
    db = streaming.util.create_streaming_db()
    c = db.changes(feed="longpoll", since=BAD_SINCE)
    # The number of docs on each shard is random but should
    # theoretically be more than 50 and less than 800
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(greater_than(50)))
    assert_that(c.results, has_length(less_than(800)))


def test_changes_continuous_with_since_replacement():
    db = streaming.util.create_streaming_db()
    c = db.changes(feed="continuous", since=BAD_SINCE, timeout=500)
    # The number of docs on each shard is random but should
    # theoretically be more than 50 and less than 800
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(greater_than(50)))
    assert_that(c.results, has_length(less_than(800)))


def test_changes_continuous_with_heartbeat():
    db = streaming.util.create_streaming_db()
    r = db.srv.res.get(db.path("_changes"), stream=True, params={
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
    assert_that(changes_read, is_(streaming.util.NUM_DOCS))
    assert_that(heart_beats, is_(3))
    r.close()


def test_changes_continuous_with_heartbeat_and_since():
    db = streaming.util.create_streaming_db()
    last_seq = db.changes().last_seq

    r = db.srv.res.get(db.path("_changes"), stream=True, params={
        "feed":"continuous",
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
    r.close()


def test_changes_with_seq_interval():
    db = streaming.util.create_streaming_db()
    seq_interval = 3
    changes_read = 0
    c = db.changes(seq_interval=seq_interval)
    for row in c.results:
        changes_read += 1
        if changes_read % seq_interval == 2:
            assert_that(row.get("seq"), is_not(None))
        else:
            assert_that(row, has_entry("seq", None))

    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(streaming.util.NUM_DOCS))
    assert_that(c.results, only_contains(has_key("seq")))
