
import uuid

from hamcrest import assert_that, is_
from quimby.util.compression import gzip
from quimby.util.matchers import is_accepted, is_ok
from quimby.util.test import DbPerClass


class DocGzipTests(DbPerClass):

    def test_gzipped_attachments(self):
        doc = {'_id': uuid.uuid4().hex}
        self.db.doc_save(doc)
        headers = {
            "Content-Encoding": "gzip",
            "Content-Type": "text/html"
        }
        body = '<html><body><blink>hello</blink></body></html>'
        data = gzip(body)

        path = self.db.path("%s/html" % doc["_id"])

        with self.res.return_errors():
            p = {"rev": doc["_rev"]}
            r = self.res.put(path, params=p, headers=headers, data=data)

        assert_that(r.status_code, is_accepted)

        with self.res.return_errors():
            r = self.res.get(path)

        assert_that(r.status_code, is_ok)
        assert_that(r.headers['Content-Type'], is_('text/html'))
        assert_that(r.text, is_(body))
