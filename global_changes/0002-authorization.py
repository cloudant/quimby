
import time
import random
import sys

from hamcrest import *

import cloudant


USERS = ["user_a", "user_b", "user_c"]
LIMITS = ["user_limit_a", "user_limit_b"]
UNAUTHED = ["unauth_user_a", "unauth_user_b", "unauth_user_c"]

def setup():
    srv = cloudant.get_server()
    db = srv.db("global_changes")
    if not db.exists():
        db.create()
    db = srv.db("test_suite_db_global_changes")
    if not db.exists():
        db.create()
    for user in USERS + LIMITS:
        if not srv.user_exists(user):
            srv.user_create(user, user, "foo@bar.com", roles=['_db_updates'])
        with srv.user_context(user, user):
            db = srv.db("db_%s" % user)
            db.reset(q=1)
    for user in UNAUTHED:
        if not srv.user_exists(user):
            srv.user_create(user, user, "foo@bar.com")
        with srv.user_context(user, user):
            db = srv.db("db_%s" % user)
            db.reset(q=1)
    time.sleep(0.5)


def test_admin_sees_all():
    srv = cloudant.get_server()
    c = srv.global_changes()
    assert_that(c.results, has_item(has_entry("account", "_admin")))
    assert_that(c.results, has_item(has_entry("account", "user_a")))
    assert_that(c.results, has_item(has_entry("account", "user_b")))
    assert_that(c.results, has_item(has_entry("account", "user_c")))


def test_unauthorized_sees_nothing():
    for user in USERS:
        srv = cloudant.get_server(user=user, auth=())
        try:
            c = srv.global_changes()
        except Exception as e:
            assert_that(e.response.status_code, is_(401))
        else:
            assert_that(True, is_(False))


@cloudant.skip_test(reason="BROKEN TEST - FB 31024")
def test_bad_role_sees_nothing():
    srv = cloudant.get_server()
    for user in UNAUTHED:
        with srv.user_context(user, user):
            try:
                c = srv.global_changes()
            except Exception as e:
                assert_that(e.response.status_code, is_(403))
            else:
                assert_that(True, is_(False))


def test_scoped_to_user():
    srv = cloudant.get_server()
    for user in USERS:
        with srv.user_context(user, user):
            dbname = "db_%s" % user
            c = srv.global_changes()
            assert_that(c.results, only_contains(has_entry("dbname", dbname)))


def test_limit_as_admin_and_non_admin():
    srv = cloudant.get_server()

    for _ in xrange(2):
        for user in LIMITS:
            with srv.user_context(user, user):
                dbid = random.randint(0, sys.maxint)
                dbname = "test_suite_db_global_changes_%d" % dbid
                db = srv.db(dbname)

                db.create(q=1)
                db.doc_save({"foo":"bar"})

                db.delete()

    c = srv.global_changes(limit=5, timeout=500)
    assert_that(c.results, has_length(5))

    c = srv.global_changes(limit=0, timeout=500)
    assert_that(c.results, has_length(0))

    for user in LIMITS:
        with srv.user_context(user, user):
            c = srv.global_changes(limit=5, timeout=500)
            assert_that(c.results, has_length(5))

            c = srv.global_changes(limit=0, timeout=500)
            assert_that(c.results, has_length(0))
