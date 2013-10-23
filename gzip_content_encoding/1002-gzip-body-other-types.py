
import uuid
import json

from hamcrest import *

import cloudant

def test_gzipped_body_jpeg_content_type():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset()
    docid = str(uuid.uuid4())
    doc = {'id': docid}
    db.doc_save(doc)
    html = '<html><body>hello</body></html>'
    data = cloudant.gzip(html)
    headers = {"Content-Encoding": "gzip", "Content-Type": "text/html"}
    path = db.path("%s/html" % docid)
    try:
        resp = db.srv.res.put(path, headers=headers, data=data)
    except Exception as http_err:
        resp = http_err.response
    assert_that(resp.status_code, is_(201))
    try:
        resp = db.srv.res.get(path)
    except Exception as http_err:
        resp = http_err.response
    assert_that(resp.status_code, is_(200))
    assert_that(resp.text, is_(html))
    # cast CaseInsensitiveDict to dict
    assert_that(dict(resp.headers), has_entry("content-type", "text/html"))
