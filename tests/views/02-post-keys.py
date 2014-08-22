
import textwrap

from hamcrest import assert_that, has_entry, has_length
from quimby.util.test import DbPerClass


NUM_DOCS = 25


class ViewAPITests(DbPerClass):

    Q = 8

    @classmethod
    def setUpClass(klass):
        self.db.doc_save(self.ddoc())
        docs = [
            {"_id": "a", "key": "a", "vals": 1},
            {"_id": "b", "key": "b", "vals": 1},
            {"_id": "c", "key": "c", "vals": 3},
            {"_id": "d", "key": "a", "vals": 1},
            {"_id": "e", "key":   1, "vals": 2},
            {"_id": "f", "key": "f", "vals": 1}
        ]
        self.db.bulk_docs(docs)

    def test_empty_keys(self):
        self.run([], [])

    def test_missing_key(self):
        self.run(["z"], [], reduce_keys=[])

    def test_single_unique_key(self):
        self.run(["b"], ["b"])

    def test_one_key_present_one_key_missing(self):
        self.run(["b", "z"], ["b"], reduce_keys=[])

    def test_one_key_present_one_key_missing_different_order(self):
        self.run(["z", "b"], ["b"], reduce_keys=["b"])

    def test_multiple_keys(self):
        self.run(["b", "f"], ["b", "f"])

    def test_multiple_keys_different_order(self):
        self.run(["f", "b"], ["f", "b"])

    def test_repeated_keys(self):
        self.run(["b", "b"], ["b", "b"], skip_reduce=True)

    def test_single_key_multiple_rows(self):
        self.run(["a"], [("a", "a"), ("d", "a")])

    def test_repeated_key_with_multiple_rows(self):
        # This needs to change after BugzId: 23155
        self.run(
            ["a", "a"],
            [("a", "a"), ("a", "a"), ("d", "a"), ("d", "a")],
            skip_reduce=True
        )

    def test_bad_fabric_error(self):
        # This needs to change after BugzId: 23155
        keys = ["a", "b", "a"]
        expect = ["b", ("a", "a"), ("a", "a"), ("d", "a"), ("d", "a")]
        self.run(keys, expect, skip_reduce=True)

    def test_multiple_multiple_rows(self):
        # This needs to change after BugzId: 23155
        keys = ["a", "a", "c"]
        expect = [
            ("a", "a"),
            ("a", "a"),
            ("d", "a"),
            ("d", "a"),
            "c",
            "c",
            "c"
        ]
        self.run(keys, expect, skip_reduce=True)

    def test_multiples_with_repeated(self):
        keys = ["a", "c", "b", "b"]
        expect = [("a", "a"), ("d", "a"), "c", "c", "c", "b", "b"]
        self.run(keys, expect, skip_reduce=True)

    def run(self, keys, expect, skip_reduce=False, reduce_keys=None):
        # Check map view keys
        v = self.db.view("foo", "bar", keys=keys)
        assert_that(v.rows, has_length(len(expect)))
        for i, k in enumerate(expect):
            if isinstance(k, tuple):
                docid, key = k
            else:
                docid, key = (k, k)
            assert_that(v.rows[i], has_entry("id", docid))
            assert_that(v.rows[i], has_entry("key", key))
        # Check reduce view keys
        if skip_reduce:
            # Only use skip_reduce on repeated keys tests until
            # we fix FB 24373
            return
        if reduce_keys is not None:
            keys = reduce_keys
        v = self.db.view("foo", "bam", keys=keys, group=True)
        assert_that(v.rows, has_length(len(keys)))
        for i, k in enumerate(keys):
            assert_that(v.rows[i], has_entry("key", k))

    def ddoc():
        bar_map = textwrap.dedent("""\
        function(doc) {
            if(!doc.key) {
                emit(doc._id, null);
                return;
            }
            for(var i = 0; i < doc.vals; i++) {
                emit(doc.key, null);
            }
        }""")
        bam_map = textwrap.dedent("""\
        function(doc) {
            if(!doc.key) {
                emit(doc._id, 0);
                return;
            }
            for(var i = 0; i < doc.vals; i++) {
                emit(doc.key, 1);
            }
        }
        """)
        bam_red = "_sum"
        return {
            "_id": "_design/foo",
            "views": {
                "bar": {
                    "map": bar_map
                },
                "bam": {
                    "map": bam_map,
                    "reduce": bam_red
                }
            }
        }
