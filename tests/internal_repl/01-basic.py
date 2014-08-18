import base64
import hashlib
import itertools
import json
import struct
import time
import uuid

import cloudant

from hamcrest import *


def b64url(val):
    term = chr(131) + chr(109) + struct.pack("!I", len(val)) + str(val)
    md5 = hashlib.md5(term).digest()
    b64 = base64.b64encode(md5)
    return b64.rstrip("=").replace("/", "_").replace("+", "-")


def mk_docid(src_val, tgt_val):
    n1 = b64url(src_val)
    n2 = b64url(tgt_val)
    return "_local/shard-sync-{0}-{1}".format(n1, n2)


def test_basic_internal_replication():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset(q=1)

    private_nodes = cloudant.nodes()

    dbsdb = private_nodes[0].db("dbs")
    dbdoc = dbsdb.doc_open("test_suite_db")
    suffix = "".join(map(chr, dbdoc["shard_suffix"]))

    pdbname = "shards%2F00000000-ffffffff%2Ftest_suite_db" + suffix

    srcdb = private_nodes[0].db(pdbname)
    tgtdbs = [s.db(pdbname) for s in private_nodes[1:]]

    def make_docs(count):
        ret = []
        for i in range(count):
            ret.append({"_id": uuid.uuid4().hex})
        return ret
    for i in range(10):
        srcdb.bulk_docs(make_docs(100))

    total_docs = srcdb.info()["doc_count"]

    for tdb in tgtdbs:
        i = 0
        while tdb.info()["doc_count"] < total_docs:
            i += 1
            if i > 32:
                raise AssertionError("Timeout during internal replication")
            time.sleep(0.25)
    # There's a race with the next tests on
    # who writes/reads the _local doc first.
    time.sleep(0.25)

    for (src, tgt) in itertools.permutations(private_nodes, 2):
        sdb = src.db(pdbname)
        tdb = tgt.db(pdbname)
        docid = mk_docid(sdb.info()["uuid"], tdb.info()["uuid"])
        doc1 = sdb.doc_open(docid)
        doc2 = tdb.doc_open(docid)
        assert_that(doc1, is_(doc2))
        assert_that(doc1, has_key("seq"))
        assert_that(doc1["seq"], is_(total_docs))
        assert_that(doc1, has_key("history"))
        assert_that(doc1["history"], has_length(equal_to(1)))
        assert_that(doc1["history"].values()[0], has_length(greater_than(0)))
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

