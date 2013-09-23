
from hamcrest import *

import cloudant


USERS = ["user_a", "user_b", "user_c"]

def setup():
    srv = cloudant.get_server()
    db = srv.db("global_changes")
    if not db.exists():
        db.create()
    db = srv.db("test_suite_db_global_changes")
    if not db.exists():
        db.create()
    for user in USERS:
        if not srv.user_exists(user):
            srv.user_create(user, user, "foo@bar.com")
        with srv.user_context(user, user):
            db = srv.db("db_%s" % user)
            db.reset(q=1)


def test_admin_sees_all():
    srv = cloudant.get_server()
    c = srv.global_changes()
    assert_that(c.results, has_item(has_entry("account", "_admin")))
    assert_that(c.results, has_item(has_entry("account", "user_a")))
    assert_that(c.results, has_item(has_entry("account", "user_b")))
    assert_that(c.results, has_item(has_entry("account", "user_c")))


def test_scoped_to_user():
    srv = cloudant.get_server()
    for user in USERS:
        with srv.user_context(user, user):
            dbname = "db_%s" % user
            c = srv.global_changes()
            assert_that(c.results, only_contains(has_entry("name", dbname)))


