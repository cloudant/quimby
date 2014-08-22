

import uuid

from hamcrest import assert_that, has_length, only_contains
from quimby.util.matchers import \
    is_accepted, \
    is_bad_request, \
    is_precondition_failed

from quimby.util.matchers import is_ok, is_not_found
from quimby.util.test import DbPerTest, random_db_name, requires


class DbCreateDeleteTest(DbPerTest):

    def test_create(self):
        with self.res.return_errors():
            r = self.res.put(random_db_name())
            assert_that(r.status_code, is_accepted)

    @requires("clusters")
    def test_default_n_and_q(self):
        db = self.srv.db(random_db_name())
        db.create(q=8, n=3)
        shards = db.shards()
        assert_that(shards, has_length(8))
        assert_that(shards.values(), only_contains(has_length(3)))

    @requires("cluster")
    def create_with_q(self):
        db_name = uuid.uuid4().hex

        with self.res.return_errors():
            r = self.res.put(db_name, params={"q": "1"})
            assert_that(r.status_code, is_accepted)

            r = self.res.get(db_name + "/_shards")
            assert_that(r.status_code, is_ok)
            shards = r.json()["shards"]
            assert_that(shards, has_length(1))

    @requires("cluster")
    def create_with_n(self):
        db_name = uuid.uuid4().hex

        with self.return_errors():
            r = self.res.put(db_name, params={"q": 1, "n": 1})
            assert_that(r.status_code, is_accepted)

            r = self.res.get(db_name + "/_shards")
            assert_that(r.status_code, is_ok)
            shards = r.json()["shards"]
            assert_that(shards, has_length(1))
            assert_that(shards[0], has_length(1))

    def test_failure_on_bad_name(self):
        with self.res.return_errors():
            r = self.res.put("INVALID")
            assert_that(r.status_code, is_bad_request)

            r = self.res.put("_foobar")
            assert_that(r.status_code, is_bad_request)

    def test_failure_when_exists(self):
        with self.res.return_errors():
            r = self.res.put(self.db.name)
            assert_that(r.status_code, is_precondition_failed)

    def test_delete_non_existent(self):
        with self.res.return_errors():
            r = self.res.delete(uuid.uuid4().hex)
            assert_that(r.status_code, is_not_found)

    def test_delete_existing_db(self):
        with self.res.return_errors():
            r = self.res.delete(self.db.name)
            assert_that(r.status_code, is_accepted)
