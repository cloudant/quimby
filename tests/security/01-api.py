
import json
import unittest

from hamcrest import assert_that, has_entry, is_
from quimby.util.matchers import is_accepted
from quimby.util.test import DbPerTest, random_db_name, requires

import quimby.client

USER = "foo"
ROLES = ["bar", "bam"]


class SecurityAPITests(DbPerTest):

    @classmethod
    def setUpClass(klass):
        super(SecurityAPITests, klass).setUpClass()
        srv = quimby.client.default_server()
        try:
            srv.db("_users").create(q=1)
        except:
            pass
        if not srv.user_exists("foo"):
            srv.user_create(
                USER,
                USER,
                "foo@bar.com",
                roles=ROLES
            )

    def test_get_security(self):
        with self.res.return_errors():
            r = self.res.get(self.db.path("_security"))

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), is_({}))

    def test_put_security(self):
        sec_props = {
            "admins": {"roles": [], "names": []},
            "members": {"roles": [], "names": []}
        }

        with self.res.return_errors():
            d = json.dumps(sec_props)
            r = self.res.put(self.db.path("_security"), data=d)

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_entry("ok", True))

    def test_empty_security(self):
        self.do_check({}, 200, 200)

    def test_members_name(self):
        sec = {"members": {"names": ["foo"], "roles": []}}
        self.do_check(sec, 401, 200)

    def test_members_roles(self):
        sec = {"members": {"names": [], "roles": [ROLES[0]]}}
        self.do_check(sec, 401, 200)

    @unittest.skip("FIXMENOWINBETA!!!!!!!!!!!!")
    def test_members_other_role(self):
        # No idea why this fails.
        sec = {"members": {"names": [], "roles": ["carrot"]}}
        self.do_check(sec, 401, 403)

    @requires("cloudant")
    def test_minimal_readers_security_reader_role(self):
        sec = {"readers": {"names": ["foo"], "roles": ["_reader"]}}
        self.do_check(sec, 401, 403)

    @requires("cloudant")
    def test_security_reader_role_with_cloudant_nobody(self):
        sec = {
            "members": {"names": ["foo"], "roles": ["_reader"]},
            "cloudant": {"nobody": []}
        }
        self.do_check(sec, 401, 403)

    @requires("cloudant")
    def test_security_nobody_full_access_and_no_members(self):
        sec = {
            "members": {"names": [], "roles": []},
            "cloudant": {"nobody": ["_reader", "_writer", "_admin"]}
        }
        self.do_check(sec, 200, 200)

    @requires("cloudant")
    def test_security_nobody_full_access_with_members(self):
        sec = {
            "members": {"names": ["foo"], "roles": []},
            "cloudant": {"nobody": ["_reader", "_writer", "_admin"]}
        }
        self.do_check(sec, 401, 200)

    def do_check(self, sec_props, anon_code, user_code):
        self.db.set_security(sec_props)

        with self.srv.anonymous_user_context():
            with self.res.return_errors():
                r = self.res.get(self.db.name)
        assert_that(r.status_code, is_(anon_code))

        with self.srv.user_context(USER, USER):
            with self.res.return_errors():
                r = self.res.get(self.db.name)
        assert_that(r.status_code, is_(user_code))
