
from hamcrest import assert_that, has_entry, has_length, is_not
from quimby.util.matchers import is_precondition_failed
from quimby.util.test import DbPerClass

import quimby.data as data


NUM_DOCS = 100

# All nodes are node1 with an update_seq of 0
SINCE_SEQ_1 = "".join("""\
    0-g1AAAACjeJzLYWBgYMlgTmHgz8tPSTV0M
    DQy1zMAQsMcoARTIkOS_f___7MSGXAqyWMB
    kgwHgNR_olQ2QFTux6cyyQFIJtVDzMsCAEo
    dK6Y
""".split())

# All nodes are node3 with update_seq of 500
SINCE_SEQ_2 = "".join("""
    2000-g1AAAACveJzLYWBgYMlgTmHgz8tPST
    V2MDQy1zMAQsMcoARTIkOS_f___7OSGBgYv
    -BUlccCJBkOAKn_xCpugCjeT0BxkgOQTKqH
    m5oFAJG7L4Y
""".split())


class ChangesMaintenanceModeTests(DbPerClass):

    Q = 4

    @classmethod
    def setUpClass(klass):
        super(ChangesMaintenanceModeTests, klass).setUpClass()
        klass.db.bulk_docs(data.gen_docs(NUM_DOCS), w=3)

    def test_basic(self):
        self.run_changes(100)

    def test_basic_since(self):
        self.run_changes(100, since=SINCE_SEQ_1)
        self.run_changes(0, since=SINCE_SEQ_2)

    def test_longpoll(self):
        self.run_changes(100, feed="longpoll", timeout=500)

    def test_longpoll_since(self):
        self.run_changes(100, feed="longpoll", timeout=500, since=SINCE_SEQ_1)
        self.run_changes(0, feed="longpoll", timeout=500, since=SINCE_SEQ_2)

    def test_continuous(self):
        self.run_changes(100, feed="continuous", timeout=500)

    def test_continuous_since(self):
        self.run_changes(
            100,
            feed="continuous",
            timeout=500,
            since=SINCE_SEQ_1
        )
        self.run_changes(0, feed="continuous", timeout=500, since=SINCE_SEQ_2)

    def test_node_replacement(self):
        # First run the changes on a node so we get the
        # last update seq.
        n1 = self.srv.nodes()[0]
        c = n1.db(self.db.name).changes()
        assert_that(c.results, has_length(NUM_DOCS))
        last_seq = c.last_seq

        # Now put the ndoe into maintenance mode and then
        # run the changes feed again to make sure that
        # we don't get all of the updates again.
        try:
            n1.config_set("cloudant", "maintenance_mode", "true")
            n2 = self.srv.nodes()[-1]
            assert_that(n2, is_not(n1))
            c = n2.db(self.db.name).changes(since=last_seq)
            assert_that(c.results, has_length(0))
        finally:
            n1.config_set("cloudant", "maintenance_mode", "false")

    def run_changes(self, num_results, **kwargs):
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
                try:
                    c = self.db.changes(**kwargs)
                except:
                    raise
                assert_that(c.results, has_length(num_results))
            n = nodes[-1]
            n.config_set("cloudant", "maintenance_mode", "true")
            with self.res.return_errors():
                r = self.res.get(self.db.path("_changes"), params=kwargs)
            assert_that(r.status_code, is_precondition_failed)
            assert_that(r.json(), has_entry("error", "nodedown"))
        finally:
            for n in nodes:
                n.config_set("cloudant", "maintenance_mode", "false")
