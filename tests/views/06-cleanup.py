
from hamcrest import assert_that, has_entry
from quimby.util.matchers import is_accepted
from quimby.util.test import DbPerTest


class ViewCleanupAPITests(DbPerTest):

    def test_cleanup(self):
        with self.res.return_errors():
            r = self.res.post(self.db.path("_view_cleanup"))

        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_entry("ok", True))
