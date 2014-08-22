
import unittest

from hamcrest import assert_that, has_entry, has_item, has_length, is_
from quimby.util.test import DbPerClass


class DisableGlobalUpdatesTests(DbPerClass):

    @unittest.skip("Too much racey.")
    def test_disable(self):
        # First check that global changes is enabled. We need to specify
        # a seq here in case we just created the db. Longpoll might end
        # up returning just the creation.
        seq = self.srv.global_changes().last_seq
        c = self.srv.global_changes(feed="longpoll", since=seq, timeout=5000)
        seq = c.last_seq

        self.db.doc_save({})
        c = self.srv.global_changes(feed="longpoll", since=seq, timeout=5000)
        assert_that(c.results, has_item(has_entry("dbname", self.db.name)))

        seq = c.last_seq

        # Disable global_changes
        for node_srv in self.srv.nodes():
            node_srv.config_set("global_changes", "update_db", "false")

        try:
            # Wait for the feed to quiet down
            c = self.srv.global_changes(
                feed="continuous",
                since=seq,
                timeout=2000
            )
            seq = c.last_seq

            # Update again
            self.db.doc_save({})
            c = self.srv.global_changes(
                feed="longpoll",
                since=seq,
                timeout=2000
            )
            assert_that(c.results, has_length(0))
            assert_that(c.last_seq, is_(seq))

        finally:
            # Re-enable global changes
            for srv in self.srv.nodes():
                srv.config_set("global_changes", "update_db", "true")
