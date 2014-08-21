
from hamcrest import \
    assert_that, \
    greater_than, \
    has_entry, \
    has_key, \
    instance_of, \
    is_

from quimby.util.matchers import is_accepted
from quimby.util.test import DbPerClass


class DbCommandsAPITest(DbPerClass):

    def test_ensure_full_commit(self):
        with self.res.return_errors():
            r = self.res.post(self.db.path("_ensure_full_commit"))

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_entry("ok", True))
        assert_that(r.json(), has_key("instance_start_time"))

    def test_get_revs_limit(self):
        with self.res.return_errors():
            r = self.res.get(self.db.path("_revs_limit"))

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), instance_of(int))
        assert_that(r.json(), greater_than(0))

    def test_put_revs_limit(self):
        revs_limit = 1200

        with self.res.return_errors():
            d = str(revs_limit)
            r = self.res.put(self.db.path("_revs_limit"), data=d)

        assert_that(r.status_code, is_accepted)

        with self.res.return_errors():
            r = self.res.get(self.db.path("_revs_limit"))

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), is_(revs_limit))

    # def test_missing_revs(self):
    #     # TODO: Need to understand this better before can implement
    #     pass

    # def test_revs_diff(self):
    #     # TODO: Need to understand this better before can implement
    #     pass
