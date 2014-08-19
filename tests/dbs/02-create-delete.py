

from hamcrest import *
from quimby.util.matchers import *
from quimby.testuitl import *


class DbCreateDeleteTest(unittest.TestCase):

    @setup_random_db()
    def setUp(self):
        pass

    def test_create(self):
        with self.res.return_errors():
            r = self.res.put(uuid.uuid4().hex)
            assert_that(r.status_code, is_accepted)

    @cluster_test
    def create_with_q(self):
        db_name = uuid.uuid4().hex
        
        with self.res.return_errors():
            r = self.res.put(db_name, params={"q":"1"})
            assert_that(r.status_code, is_accepted)

            r = self.res.get(db_name + "/_shards")
            assert_that(r.status_code, is_ok)
            shards = r.json()["shards"]
            assert_that(shards, has_length(1))

    @cluster_test
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
            r = self.res.put(self.db_name)
            assert_that(r.status_code, is_precondition_failed)

    def test_delete_non_existent(self):
        with self.res.return_errors():
            r = self.res.delete(uuid.uuid4().hex)
            assert_that(r.status_code, is_not_found)

    def test_delete_existing_db(self):
        with self.res.return_errors():
            r = self.res.delete(self.db_name)
            assert_that(r.status_code, is_accepted)
