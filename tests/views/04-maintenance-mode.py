
from hamcrest import assert_that, has_entry, has_length
from quimby.util.matchers import is_error
from quimby.util.test import DbPerClass

import quimby.data as data


NUM_ROWS = 100


class ViewMaintenanceModeTests(DbPerClass):

    def __init__(self, *args, **kwargs):
        super(ViewMaintenanceModeTests, self).__init__(*args, **kwargs)
        self.db.bulk_docs(data.gen_docs(NUM_ROWS), w=3)
        self.db.doc_save(data.SIMPLE_MAP_RED_DDOC, w=3)

    def test_map(self):
        self.run("bar")

    def test_map_stale_ok(self):
        self.run("bar", stale="ok")

    def test_reduce(self):
        self.run("bam")

    def test_reduce_stale_ok(self):
        self.run("bam", stale="ok")

    def run(self, view, **kwargs):
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
                assert_that(v.rows, has_length(NUM_ROWS))
            n = nodes[-1]
            n.config_set("cloudant", "maintenance_mode", "true")
            with self.res.return_errors():
                p = self.db.path("_design", "foo", "_view", view)
                r = self.res.get(p, **kwargs)
            assert_that(r.status_code, is_error)
            assert_that(r.json(), has_entry("error", "nodedown"))
        finally:
            for n in nodes:
                n.config_set("cloudant", "maintenance_mode", "false")
