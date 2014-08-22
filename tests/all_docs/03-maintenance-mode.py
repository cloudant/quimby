

from hamcrest import assert_that, has_entry, has_length
from quimby.util.matchers import is_precondition_failed
from quimby.util.test import DbPerClass, requires

import quimby.data as data


NUM_DOCS = 150


@requires("cluster")
class AllDocsMaintenanceModeTests(DbPerClass):

    @classmethod
    def setUpClass(klass):
        self.db.bulk_docs(data.gen_docs(NUM_DOCS), w=3)

    def test_maintenance_mode(self):
        # Check that we can run with maintenance mode on a number
        # of servers. This has an assumption that the last node in
        # the cluster has a full shard ring. To make this better
        # we should compare the list of nodes returned to the shard
        # map so that we can remove non-shard-containing nodes from
        # service first.
        nodes = self.srv.get_nodes()
        try:
            for n in nodes[:-1]:
                n.config_set("cloudant", "maintenance_mode", "true")
                v = self.db.all_docs()
                assert_that(v.rows, has_length(100))
            nodes[-1].config_set("cloudant", "maintenance_mode", "true")
            with self.res.return_errors():
                r = self.res.get(self.db.path("/_all_docs"))
            assert_that(r.status_code, is_precondition_failed)
            assert_that(r.json(), has_entry("error", "nodedown"))
        finally:
            for n in nodes:
                n.config_set("cloudant", "maintenance_mode", "false")
