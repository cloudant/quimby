
import random
import sys
import uuid


import cloudant


NUM_DOCS = 1001
NUM_MAP_ROWS = 1000
NUM_RED_ROWS = (1, 8, 1000)


def load_ddoc(db):
    ddoc = {
        "_id": "_design/foo",
        "views": {
            "bar": {
                "map": "function(doc) {emit(doc.key, doc.val);}"
            },
            "bam": {
                "map": "function(doc) {emit(doc.key, doc.val);}",
                "reduce": "_sum"
            }
        }
    }
    if not db.doc_exists(ddoc["_id"]):
        db.doc_save(ddoc)
    else:
        old_ddoc = db.doc_open(ddoc["_id"])
        ddoc["_rev"] = old_ddoc["_rev"]
        if ddoc != old_ddoc:
            db.doc_save(ddoc)


def load_data(db):
    def make_docs(count):
        for i in range(count):
            yield {
                "_id": uuid.uuid4().hex,
                "key": [random.randint(1, 8), random.randint(0, sys.maxint)],
                "val": 1
            }
    num_docs = db.info()["doc_count"]
    assert 1 <= num_docs <= NUM_DOCS
    batch = []
    for doc in make_docs(NUM_DOCS - num_docs):
        batch.append(doc)
        if len(batch) >= 100:
            db.bulk_docs(batch)
            batch = []
    if len(batch):
        db.bulk_docs(batch)


def create_streaming_db():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db_streaming")
    if not db.exists():
        db.create()
    load_ddoc(db)
    load_data(db)
    return db


