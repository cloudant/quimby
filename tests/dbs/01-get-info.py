
from hamcrest import assert_that, has_entry, has_items
from quimby.util.matchers import is_accepted
from quimby.util.test import DbPerTest


class DbInfoTest(DbPerTest):

    def setUp(self):
        super(DbInfoTest, self).setUp(q=1)

    def test_get_db_metadata(self):
        with self.res.return_errors():
            r = self.res.get(self.db.path(""))

        assert_that(r.status_code, is_accepted)

        items = [
            "compact_running",
            "db_name",
            "disk_format_version",
            "disk_size",
            "doc_count",
            "doc_del_count",
            "instance_start_time",
            "other",
            "purge_seq",
            "update_seq"
        ]
        assert_that(r.json().keys(), has_items(items))

        expect_db_name = self.db.name.replace("%2f", "/")
        assert_that(r.json(), has_entry("db_name", expect_db_name))
