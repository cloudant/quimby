
import json

from hamcrest import assert_that, has_entry, is_
from quimby.util.matchers import is_accepted
from quimby.util.test import DbPerTest, random_db_name, requires


USER = "foo"
ROLES = ["bar", "bam"]


class SecurityAPITests(DbPerTest):

    def setUp(self):
        super(SecurityAPITests, self).setUp(q=1)
        users_db = random_db_name()
        for n in self.srv.nodes():
            n.config_set("chttpd_auth", "authentication_db", users_db)
            n.config_set("couch_httpd_auth", "authentication_db", users_db)
        self.srv.db(users_db).recycle(q=1)
        if not self.srv.user_exists("foo"):
            self.srv.user_create(
                USER,
                USER,
                "foo@bar.com",
                roles=ROLES,
                dbname=users_db
            )

    def tearDown(self):
        for n in self.srv.nodes():
            n.config_set("chttpd_auth", "authentication_db", "_users")
            n.config_set("couch_httpd_auth", "authentication_db", "_users")

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
        self.run({}, 401, 403)

    def test_members_name(self):
        sec = {"members": {"names": ["foo"], "roles": []}}
        self.run(sec, 401, 403)

    def test_members_roles(self):
        sec = {"members": {"names": [], "roles": [ROLES[0]]}}
        self.run(sec, 401, 200)

    def test_members_other_role(self):
        sec = {"members": {"names": [], "roles": ["carrot"]}}
        self.run(sec, 401, 403)

    @requires("cloudant")
    def test_minimal_readers_security_reader_role(self):
        sec = {"readers": {"names": ["foo"], "roles": ["_reader"]}}
        self.run(sec, 401, 403)

    @requires("cloudant")
    def test_security_reader_role_with_cloudant_nobody(self):
        sec = {
            "members": {"names": ["foo"], "roles": ["_reader"]},
            "cloudant": {"nobody": []}
        }
        self.run(sec, 401, 403)

    @requires("cloudant")
    def test_security_nobody_full_access_and_no_members(self):
        sec = {
            "members": {"names": [], "roles": []},
            "cloudant": {"nobody": ["_reader", "_writer", "_admin"]}
        }
        self.run(sec, 200, 200)

    @requires("cloudant")
    def test_security_nobody_full_access_with_members(self):
        sec = {
            "members": {"names": ["foo"], "roles": []},
            "cloudant": {"nobody": ["_reader", "_writer", "_admin"]}
        }
        self.run(sec, 401, 200)

    def run(self, sec_props, anon_code, user_code):
        self.db.set_security(sec_props)

        with self.srv.anonymous_user_ctx():
            with self.res.return_errors():
                r = self.res.get(self.db.name)
        assert_that(r.status_code, is_(anon_code))

        with self.srv.user_ctx(USER, USER):
            with self.res.return_errors():
                r = self.res.get(self.db.name)
        assert_that(r.status_code, is_(user_code))
