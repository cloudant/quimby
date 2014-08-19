
from hamcrest import *
from quimby.util.matchers import *
from quimby.util.test import *


class DbInfoTest(unittest.TestCase):

    @setup_random_db()
    def setUp(self):
        pass

    def test_get_db_metadata(self):
        with self.res.return_errors():
            r = self.res.get(self.db_name)

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
        assert_that(response.json().keys(), has_items(items))

        expect_db_name = self.db_name.replace("%2f", "/")
        assert_that(response.json(), has_entry("db_name", expect_db_name))
