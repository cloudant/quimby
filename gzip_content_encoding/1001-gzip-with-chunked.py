
import uuid
import json

from hamcrest import *

import cloudant

def _chunked_resp(string, chunk_size):
    for i in xrange(0, len(string), chunk_size):
        yield string[i:i+chunk_size]


def test_gzipped_body_chunked_upload():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset()
    docs = [{'id': str(uuid.uuid4())} for _ in xrange(30)]
    data = cloudant.gzip(json.dumps({'docs': docs}))
    headers = {"Content-Encoding": "gzip", "Content-Type": "application/json"}
    path = db.path("_bulk_docs")
    chunked_resp_gen = _chunked_resp(data, 50)
    try:
        # Setting data as a generator automatically sets chunked encoding
        # header.
        resp = db.srv.res.post(path, headers=headers, data=chunked_resp_gen)
    except Exception as http_err:
        resp = http_err.response
    assert_that(resp.status_code, is_(201))
