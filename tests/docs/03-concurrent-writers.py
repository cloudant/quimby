
import time
import uuid

from concurrent import futures
from hamcrest import assert_that, greater_than, has_length, only_contains
from quimby.util.test import DbPerTest, random_db_name


TEST_TIME = 5
NUM_CLIENTS = 20


class MultiClientWritersTest(DbPerTest):

    def test_multiple_clients(self):
        # Here we're testing multiple clients attempting to update
        # a single document and racing to get their update in. To
        # check this we'll have each client writing unique values to
        # the doc and on successful updates check that their value was
        # the one persisted.
        #
        # Its important to use a single node's private interface for
        # this test because otherwise we're introducing the internal
        # replication race for conflicts.
        #
        # Its also important to note that we can't actually assert that
        # we hit the code we're interested in because there's no way
        # to check when updats were in the same internal write batch.
        # Although we can check for the absence of errors which will
        # have to do for now.
        n = self.srv.nodes(public=False)[0]
        db = n.db(random_db_name())
        db.reset(q=1)

        def run_client():
            writes = 0
            failures = 0
            start = time.time()
            doc = {"_id": "test_doc"}
            while True:
                doc["value"] = uuid.uuid4().hex
                try:
                    db.doc_save(doc)
                except:
                    # Doc write failed
                    doc = db.doc_open(doc["_id"])
                else:
                    writes += 1
                    written = db.doc_open(doc["_id"], rev=doc["_rev"])
                    if written["value"] != doc["value"]:
                        failures += 1
                if time.time() - start > TEST_TIME:
                    return (writes, failures)

        wait_for = []
        with futures.ThreadPoolExecutor(max_workers=NUM_CLIENTS) as e:
            for i in range(NUM_CLIENTS):
                wait_for.append(e.submit(run_client))

        res = futures.wait(wait_for, timeout=TEST_TIME*2)

        # Assert that half of the clients made at least one write.
        docs_written = [f.result()[0] for f in res.done]
        clients_that_wrote = filter(lambda c: c > 0, docs_written)
        assert_that(
            clients_that_wrote,
            has_length(greater_than(NUM_CLIENTS / 2))
        )

        # Assert that no client had a failure
        write_failures = [f.result()[1] for f in res.done]
        assert_that(write_failures, only_contains(0))

        # And assert that everything actually finished
        assert_that(res.not_done, has_length(0))
