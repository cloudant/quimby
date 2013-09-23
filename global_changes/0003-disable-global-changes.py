
from hamcrest import *

import cloudant


def setup():
    srv = cloudant.get_server()
    db = srv.db("global_changes")
    if not db.exists():
        db.create()


def test_disable_global_changes():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db_global_changes")
    if not db.exists():
        db.create(q=1)

    # First check that global changes is enabled
    seq = srv.global_changes().last_seq
    db.doc_save({})
    c = srv.global_changes(feed="longpoll", since=seq, timeout=5000)
    assert_that(c.results, has_item(has_entry("name", db.name)))
    seq = c.last_seq

    # Disable global_changes
    for node_srv in cloudant.nodes():
        node_srv.config_set("global_changes", "update_db", "false")

    try:
        # Wait for the feed to quiet down
        c = srv.global_changes(feed="continuous", since=seq, timeout=2000)
        seq = c.last_seq

        # Update again
        db.doc_save({})
        c = srv.global_changes(feed="longpoll", since=seq, timeout=2000)
        assert_that(c.results, has_length(0))
        assert_that(c.last_seq, is_(seq))
    finally:
        # Re-enable global changes
        for srv in cloudant.nodes():
            srv.config_set("global_changes", "update_db", "true")
