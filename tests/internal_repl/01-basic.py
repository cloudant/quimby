import base64
import hashlib
import itertools
import struct

from hamcrest import \
    assert_that, \
    contains_string, \
    equal_to, \
    greater_than, \
    has_entries, \
    has_key, \
    has_length, \
    instance_of, \
    is_, \
    only_contains

from quimby.util.test import DbPerTest

import quimby.data as data


class InternalReplicationTests(DbPerTest):

    def test_basic_internal_replication(self):
        private_nodes = [n.private() for n in self.srv.nodes()]

        dbsdb = private_nodes[0].db("dbs")
        dbdoc = dbsdb.doc_open(self.db.name)
        suffix = "".join(map(chr, dbdoc["shard_suffix"]))

        pdbname = "shards%2F00000000-ffffffff%2F" + self.db.name + suffix

        srcdb = private_nodes[0].db(pdbname)

        # Grab the nodes containing a copy of the shard
        tgtdbs = []
        for n in private_nodes[1:]:
            db = n.db(pdbname)
            if db.exists():
                tgtdbs.append(db)

        # Write some data that we'll then make sure gets
        # replicated out to the shard copies
        for i in range(10):
            srcdb.bulk_docs(data.gen_docs(100))

        total_docs = srcdb.info()["doc_count"]

        # Wait for our target databases to finish
        # synchronizing
        for i in range(5):
            for tdb in tgtdbs:
                tdb.changes(feed="continuous", timeout=1000).read()
            synced = True
            for tdb in tgtdbs:
                if tdb.info()["doc_count"] < total_docs:
                    synced = False
            if synced:
                break
        assert_that(synced, is_(True))

        for (src, tgt) in itertools.permutations(private_nodes, 2):
            sdb = src.db(pdbname)
            tdb = tgt.db(pdbname)
            docid = self.mk_docid(sdb.info()["uuid"], tdb.info()["uuid"])
            doc1 = sdb.doc_open(docid)
            doc2 = tdb.doc_open(docid)
            assert_that(doc1, is_(doc2))
            assert_that(doc1, has_key("seq"))
            assert_that(doc1["seq"], is_(total_docs))
            assert_that(doc1, has_key("history"))
            assert_that(doc1["history"], has_length(equal_to(1)))
            assert_that(
                doc1["history"].values()[0],
                has_length(greater_than(0))
            )
            entry = has_entries({
                "source_node": contains_string("@"),
                "source_uuid": has_length(32),
                "source_seq": greater_than(0),
                "target_node": contains_string("@"),
                "target_uuid": has_length(32),
                "target_seq": greater_than(0),
                "timestamp": instance_of(basestring)
            })
            assert_that(doc1["history"].values()[0], only_contains(entry))

    def b64url(self, val):
        term = chr(131) + chr(109) + struct.pack("!I", len(val)) + str(val)
        md5 = hashlib.md5(term).digest()
        b64 = base64.b64encode(md5)
        return b64.rstrip("=").replace("/", "_").replace("+", "-")

    def mk_docid(self, src_val, tgt_val):
        n1 = self.b64url(src_val)
        n2 = self.b64url(tgt_val)
        return "_local/shard-sync-{0}-{1}".format(n1, n2)
