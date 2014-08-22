
from hamcrest import assert_that, has_entry, has_length
from quimby.util.matchers import is_error
from quimby.util.test import DbPerClass

import quimby.data as data


NUM_ROWS = 200


class ViewMaintenanceModeTests(DbPerClass):

    @classmethod
    def setUpClass(klass):
        super(ViewMaintenanceModeTests, klass).setUpClass()
        klass.db.bulk_docs(data.gen_docs(NUM_ROWS), w=3)
        klass.db.doc_save(data.simple_map_red_ddoc(), w=3)

    def test_map(self):
        self.do_check("bar", NUM_ROWS)

    def test_map_stale_ok(self):
        self.do_check("bar", NUM_ROWS, stale="ok")

    def test_reduce(self):
        self.do_check("bam", 1)

    def test_reduce_stale_ok(self):
        self.do_check("bam", 1, stale="ok")

    def do_check(self, view, nrows, **kwargs):
        # Check that we can run with maintenance mode on a number
        # of servers. This has an assumption that the last node in
        # the cluster has a full shard ring. To make this more better
        # we should compare the list of nodes returned to the shard
        # map so that we can remove non-shard-containing nodes from
        # service first.
        nodes = self.srv.nodes()
        try:
            for n in nodes[:-1]:
                n.config_set("cloudant", "maintenance_mode", "true")
                v = self.db.view("foo", view, **kwargs)
                assert_that(v.rows, has_length(nrows))
            n = nodes[-1]
            n.config_set("cloudant", "maintenance_mode", "true")
            with self.res.return_errors():
                p = self.db.path("_design", "foo", "_view", view)
                r = self.res.get(p, params=kwargs)
            assert_that(r.status_code, is_error)
            assert_that(r.json(), has_entry("error", "nodedown"))
        finally:
            for n in nodes:
                n.config_set("cloudant", "maintenance_mode", "false")
