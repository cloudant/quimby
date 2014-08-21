
import json

from hamcrest import assert_that, has_length
from quimby.util.compression import gzip
from quimby.util.matchers import is_accepted, is_bad_request
from quimby.util.test import DbPerClass

import quimby.data as data


NUM_DOCS = 5


class GzipBulkDocsTests(DbPerClass):
    def setUp(self):
        super(GzipBulkDocsTests, self).setUp(q=1)

    def test_gzipped_body(self):
        docs = {"docs": data.gen_docs(count=NUM_DOCS)}
        r = self.bulk_docs(gzip(json.dumps(docs)), encoding="gzip")
        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_length(NUM_DOCS))

    def test_gzipped_chunked_body(self):
        docs = {"docs": data.gen_docs(count=NUM_DOCS)}
        gen = self.chunk_data(json.dumps({"docs": docs}), 25)
        r = self.bulk_docs(gen, encoding="gzip")
        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_length(NUM_DOCS))

    def test_bad_encoding(self):
        docs = {"docs": data.gen_docs(count=NUM_DOCS)}
        r = self.bulk_docs(gzip(json.dumps(docs)), encoding="bad")
        assert_that(r.status_code, is_bad_request)

    def test_bad_data(self):
        docs = {"docs": data.gen_docs(count=NUM_DOCS)}
        r = self.bulk_docs(json.dumps(docs), encoding="gzip")
        assert_that(r.status_code, is_bad_request)

    def test_no_encoding(self):
        docs = {"docs": data.gen_docs(count=NUM_DOCS)}
        r = self.bulk_docs(gzip(json.dumps(docs)))
        assert_that(r.status_code, is_bad_request)

    def bulk_docs(self, data, encoding=None):
        hdrs = {
            "Content-Type": "application/json",
        }
        if encoding is not None:
            hdrs["Content-Encoding"] = encoding
        with self.res.return_errors():
            path = self.db.path("/_bulk_docs")
            return self.res.post(path, headers=hdrs, data=data)

    def chunk_data(self, string, chunk_size):
        for i in xrange(0, len(string), chunk_size):
            yield string[i:i+chunk_size]
