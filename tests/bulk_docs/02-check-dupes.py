
import itertools
import random
import uuid

from hamcrest import assert_that, contains, has_entries, has_entry, is_
from hamcrest.library.text.stringmatches import matches_regexp
from quimby.util.test import DbPerTest


NUM_CLUSTER_TEST_CASES = 25
NUM_LOCAL_TEST_CASES = 100


class DupesInBulkDocsTests(DbPerTest):

    def setUp(self):
        super(DupesInBulkDocsTests, self).setUp(q=1)

    def test_dupes_on_cluster(self):
        self.run_ordering_check(NUM_CLUSTER_TEST_CASES)

    def test_dupes_on_local(self):
        self.run_ordering_check(NUM_LOCAL_TEST_CASES)

    def run_ordering_check(self, num_tests):
        # Create a doc for each of the possible names we'll be
        # using in this test. So far this is hard coded to "abc" both
        # here and down below in make_orderings. This is purely to
        # constrain our state space. May open this up in the future.
        src_docs = {}
        for name in "abc":
            src_docs[name] = {"_id": name}

        # An order looks like ("a", "c", "c") and is how we line
        # up repeated docs in the _bulk_docs body. We randomly sample
        # from a pool of a few thousand permutations to make sure we
        # cover all of our bases.
        for order in self.gen_cases(num_tests):

            # Notice that we make a copy of the doc so our original
            # veresions in docs_by_name aren't mutated.
            docs = [src_docs[n].copy() for n in order]

            # Add a random value so that our revision generations are
            # also random which comes into play down below. We also
            # want to occasionally test sending identical updates as well
            # so approximately every 10 tests we'll set the value to be
            # "foo".
            if random.choice(range(10)) == 0:
                for doc in docs:
                    doc["value"] = "foo"
            else:
                for doc in docs:
                    doc["value"] = uuid.uuid4().hex
            results = self.db.bulk_docs(docs)

            # The crux of our tests that asserts that the first occurence
            # of the docid is the one that's updated and any subsequent
            # repetitions are flagged as conflicts.
            assert_that(results, is_(self.correctly_ordered(order, src_docs)))

            # Since we made copies we have to update them manually
            for res in results:
                if "error" in res:
                    continue
                src_docs[res["id"]]["_rev"] = res["rev"]

            # Since all of our values are random we need to be sure
            # and check that the value persisted in the database is
            # from the expected position in the docs array.
            values = {}
            for (res, doc) in zip(results, docs):
                if "error" in res:
                    continue
                values[res["id"]] = doc["value"]
            v = self.db.all_docs(keys=values.keys(), include_docs=True)
            for row in v.rows:
                assert_that(row["doc"]["value"], is_(values[row["id"]]))

    def gen_cases(self, num_tests):
        orderings = list(self.make_orderings())
        if num_tests is None:
            for o in orderings:
                yield o
        else:
            for ordering in random.sample(orderings, num_tests):
                yield ordering

    def make_orderings(self):
        for (a, b, c) in self.gen_id_repeats():
            ids = (["a"] * a) + (["b"] * b) + (["c"] * c)
            for perm in sorted(set(itertools.permutations(ids))):
                yield perm

    def gen_id_repeats(self):
        for comb in itertools.combinations([0, 1, 2, 2, 3, 3], 3):
            yield comb

    def correctly_ordered(ordering, docs_by_name):
        seen = set()
        elems = []
        is_rev = matches_regexp(r"(\d+)-[a-fA-F0-9]{32}")
        for name in ordering:
            if name not in seen:
                docid = docs_by_name[name]["_id"]
                elems.append(has_entries({"id": docid, "rev": is_rev}))
                seen.add(name)
            else:
                elems.append(has_entry("error", "conflict"))
        return contains(*tuple(elems))
