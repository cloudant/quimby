
import uuid
import json

from hamcrest import *

import cloudant

def setup():
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    db.reset(q=1)


def _do_bulk_request(data, headers):
    srv = cloudant.get_server()
    db = srv.db("test_suite_db")
    try:
        resp = db.srv.res.post(db.path("_bulk_docs"), headers=headers, data=data)
    except Exception as http_err:
        resp = http_err.response
    return resp


def test_gzipped_body():
    docs = [{'id': str(uuid.uuid4())} for _ in xrange(7)]
    data = cloudant.gzip(json.dumps({'docs': docs}))
    headers = {"Content-Encoding": "gzip", "Content-Type": "application/json"}
    resp = _do_bulk_request(data, headers)
    assert_that(resp.status_code, is_(201))
    assert_that(len(resp.json()), is_(len(docs)))


def test_gzipped_body_bad_encoding():
    docs = [{'id': str(uuid.uuid4())} for _ in xrange(7)]
    data = cloudant.gzip(json.dumps({'docs': docs}))
    headers = {"Content-Encoding": "bad", "Content-Type": "application/json"}
    resp = _do_bulk_request(data, headers)
    assert_that(resp.status_code, is_(415))


def test_gzipped_body_bad_data():
    docs = [{'id': str(uuid.uuid4())} for _ in xrange(7)]
    data = json.dumps({'docs': docs})
    headers = {"Content-Encoding": "gzip", "Content-Type": "application/json"}
    resp = _do_bulk_request(data, headers)
    assert_that(resp.status_code, is_(400))


def test_gzipped_body_no_encoding():
    docs = [{'id': str(uuid.uuid4())} for _ in xrange(7)]
    data = cloudant.gzip(json.dumps({'docs': docs}))
    headers = {"Content-Type": "application/json"}
    resp = _do_bulk_request(data, headers)
    assert_that(resp.status_code, is_(400))

