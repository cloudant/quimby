
import json
import random
import sys
import time

from concurrent import futures
from hamcrest import *
from nose.tools import with_setup

import cloudant


def setup():
    srv = cloudant.get_server()
    db = srv.db("global_changes")
    if not db.exists():
        db.create()


def test_no_options():
    # Smoke
    srv = cloudant.get_server()
    c = srv.global_changes()
    assert_that(c, has_property("results"))
    assert_that(c, has_property("last_seq"))


@cloudant.skip_test(reason="BROKEN TEST - FB 31024")
def test_db_event_types():
    # I'm mashing each of these tests together because cycling
    # through dbs is a good way to get the dbcore nodes to hit
    # the file descriptor limit which leads to terribleness.

    srv = cloudant.get_server()
    seq = srv.global_changes().last_seq

    dbname = "test_suite_db_global_changes_%d" % random.randint(0, sys.maxint)
    db = srv.db(dbname)

    db.create(q=1)
    c = srv.global_changes(feed="continuous", since=seq, timeout=500, limit=10)
    assert_that(c.results,
            has_item(has_entries({"dbname": dbname, "type": "created"})))
    seq = c.last_seq

    db.doc_save({"foo":"bar"})
    c = srv.global_changes(feed="continuous", since=seq, timeout=500, limit=10)
    assert_that(c.results,
            has_item(has_entries({"dbname": dbname, "type": "updated"})))

    seq = c.last_seq

    # Multiple db updatse gives a single row with a different
    # update sequence
    for i in range(25):
        db.doc_save({"ohai": random.randint(0, 500)})
    c = srv.global_changes(feed="continuous", since=seq, timeout=500, limit=50)
    expect = {
        "dbname": dbname,
        "type": "updated",
        "seq": is_not(seq)
    }
    assert_that(c.results, has_item(has_entries(expect)))
    seq = c.last_seq

    db.delete()
    c = srv.global_changes(feed="continuous", since=seq, timeout=500, limit=10)
    assert_that(c.results,
            has_item(has_entries({"dbname": dbname, "type": "deleted"})))


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")  # https://gist.github.com/robfraz/3283ce59177bc561e57c
def test_limit():
    srv = cloudant.get_server()
    db = srv.db("global_changes")
    assert_that(db.info()["doc_count"], greater_than_or_equal_to(4))
    c1 = srv.global_changes(limit=2)
    assert_that(c1.results, has_length(2))
    c2 = srv.global_changes(limit=2, since=c1.last_seq)
    for row in c2.results:
        assert_that(c1.results, is_not(has_item(row)))


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")  # https://gist.github.com/robfraz/3283ce59177bc561e57c
def test_parameters_are_ignored():
    srv = cloudant.get_server()
    c = srv.global_changes(limit=1, foo="bar")
    assert_that(c.results, has_length(1))
    assert_that(c, has_property("last_seq"))


def test_400_on_bad_paramters():
    srv = cloudant.get_server()
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
    with srv.res.return_errors() as res:
        for (k, v) in tests:
            if not isinstance(v, basestring):
                v = json.dumps(v)
            r = res.get("_db_updates", params={k: v})
            assert_that(r.status_code, is_(400))


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")  # https://gist.github.com/robfraz/3283ce59177bc561e57c
def test_descending():
    srv = cloudant.get_server()
    db = srv.db("global_changes")
    assert_that(db.info()["doc_count"], greater_than_or_equal_to(4))
    c = srv.global_changes(limit=5, descending=True)
    def int_seq(res):
        return int(res["seq"].split("-", 1)[0])
    start = int_seq(c.results[0])
    # There's a bug in the underlying changes behavior that does weird
    # things to the since sequence on descending requests. We should
    # figure that out at somepoint.
    for row in c.results[1:]:
        assert_that(int_seq(row), less_than_or_equal_to(start))


def test_long_poll():
    srv = cloudant.get_server()
    c = srv.global_changes(feed="longpoll")
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, only_contains(has_key("seq")))


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")  # https://gist.github.com/robfraz/3283ce59177bc561e57c
def test_long_poll_limit():
    srv = cloudant.get_server()
    c = srv.global_changes(feed="longpoll", limit=3)
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(3))
    assert_that(c.results, only_contains(has_key("seq")))


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")
def test_long_poll_timeout():
    srv = cloudant.get_server()
    seq = srv.global_changes().last_seq

    # This is really racey if we happen to have system activity
    # or if the stats db happens to be updated in this second.
    # We may want to try turning off global changes or something.
    before = time.time()
    c = srv.global_changes(feed="longpoll", since=seq, timeout=1000)
    after = time.time()

    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(0))
    assert_that(after - before, greater_than(0.9))


def long_poll_with_update():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db_global_changes")
    if not db.exists():
        db.create()
    seq = srv.global_changes().last_seq

    def background_update():
        time.sleep(1)
        return db.doc_save({})

    with futures.ThreadPoolExecutor(max_workers=1) as e:
        f = e.submit(background_update)

    before = time.time()
    c = srv.global_changes(feed="longpoll", since=seq, timeout=5000)
    after = time.time()

    assert_that(after - before, less_than(4))

    newdoc = f.result()
    assert_that(newdoc, has_entries({"_id": anything(), "_rev": anything()}))

    assert_that(c.entries, has_item(has_entry("dbname", db.name)))


def test_continuous():
    srv = cloudant.get_server()
    c = srv.global_changes(feed="continuous", timeout=500)
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, only_contains(has_key("seq")))


@cloudant.skip_test(reason="FLAKY TEST - FB 31024")
def test_continuous_limit():
    srv = cloudant.get_server()
    c = srv.global_changes(feed="continuous", limit=4, timeout=500)
    assert_that(c, has_property("last_seq"))
    assert_that(c.results, has_length(4))
    assert_that(c.results, only_contains(has_key("seq")))


def continuous_with_update():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db_global_changes")
    if not db.exists():
        db.create()
    seq = srv.global_changes().last_seq

    def background_update():
        time.sleep(1)
        return db.doc_save({})

    with futures.ThreadPoolExecutor(max_workers=1):
        f = e.submit(background_update)

    before = time.time()
    c = srv.global_changes(feed="continuous", since=seq, timeout=2000)
    c.read()
    after = time.time()

    newdoc = f.result()
    assert_that(newdoc, has_entries({"_id": anything(), "_rev": anything()}))

    assert_that(after - before, greater_than(2))
    assert_that(c.results, has_item(has_entry("dbname", db.name)))


def test_continuous_heartbeat():
    srv = cloudant.get_server()
    r = srv.res.get("_db_updates", stream=True, params={
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

