from hamcrest import *
from hamcrest.core.core.raises import *
from requests.exceptions import HTTPError

import cloudant
from cloudant import has_header

OWNER = "cors_foo"
USER = "cors_bar"
USERS = [OWNER, USER]
SHARED_DB = "test_cors_db"

CORS_CONFIG = {
    "cors_foo": {
        "cors": {
            "enable_cors": True,
            "allow_credentials": True,
            "origins": {
                "http://baz.com": {
                    "allow_credentials": False,
                    "allow_methods": ["POST"]
                },
                "http://example.com": {}
            }
        }
    }
}

SECURITY_DOC = {
    "cloudant": {
        OWNER: [
            "_reader",
            "_writer",
            "_creator",
            "_admin"
        ],
        USER: [
            "_reader",
            "_writer"
        ]
    }
}

EXPECTED_HEADERS = "Content-Type, Accept-Ranges, Etag, Server, X-Couch-Request-ID"

def setup_module():
    srv = cloudant.get_server()
    with srv.user_context(OWNER, OWNER):
        db = srv.db(SHARED_DB)
        db.reset()
        db.set_security(SECURITY_DOC)
    for user in USERS:
        if not srv.user_exists(user):
            srv.user_create(user, user, "foo@bar.com")
        if user in CORS_CONFIG:
            assert(srv.user_config_set(user, CORS_CONFIG[user]))

def test_cors():
    srv = cloudant.get_server(auth=(USER, USER))
    origin = "http://example.com"
    db_path = "/" + SHARED_DB
    allowed_methods = "GET, HEAD, POST, PUT, DELETE, TRACE, CONNECT, COPY, OPTIONS"
    with srv.user_context(USER, USER, owner=OWNER):
        assert_that(
            calling(srv.res.options).with_args(""),
            raises(HTTPError)
        )
        db_options_resp = srv.res.options("", headers={"Origin": origin})
        assert_that(
            db_options_resp,
            has_header("Access-Control-Allow-Origin", val=origin)
        )
        assert_that(
            db_options_resp,
            has_header("Access-Control-Allow-Methods", val=allowed_methods)
        )
        assert_that(
            db_options_resp,
            has_header("Access-Control-Allow-Credentials", val="true")
        )
        assert_that(
            srv.res.get("", headers={"Origin": origin}),
            has_header("Access-Control-Allow-Origin", val=origin)
        )
        assert_that(
            calling(srv.res.get).with_args("/_all_dbs",
                headers={"Origin": origin}),
            raises(HTTPError)
        )
        db_get_resp = srv.res.get(db_path, headers={"Origin": origin})
        assert_that(
            db_get_resp,
            has_header("Access-Control-Allow-Origin", val=origin)
        )
        assert_that(
            db_get_resp,
            has_header("Access-Control-Allow-Credentials", val="true")
        )
        assert_that(
            db_get_resp,
            has_header("Access-Control-Expose-Headers", val=EXPECTED_HEADERS)
        )
        assert_that(
            srv.res.post(db_path, headers={"Origin":origin}, data="{}").json(),
            has_key("ok")
        )
        assert_that(
            srv.res.get(db_path+"/_all_docs", headers={"Origin":origin}).json(),
            has_key("rows")
        )

def test_minimal_cors():
    srv = cloudant.get_server(auth=(USER, USER))
    origin = "http://baz.com"
    db_path = "/" + SHARED_DB
    allowed_methods = "POST"
    with srv.user_context(USER, USER, owner=OWNER):
        assert_that(
            calling(srv.res.options).with_args(""),
            raises(HTTPError)
        )
        db_options_resp = srv.res.options("", headers={"Origin": origin})
        assert_that(
            db_options_resp,
            has_header("Access-Control-Allow-Origin", val=origin)
        )
        assert_that(
            db_options_resp,
            has_header("Access-Control-Allow-Methods", val=allowed_methods)
        )
        assert_that(
            db_options_resp,
            is_not(has_header("Access-Control-Allow-Credentials"))
        )
        assert_that(
            srv.res.get("", headers={"Origin": origin}),
            has_header("Access-Control-Allow-Origin", val=origin)
        )
        assert_that(
            calling(srv.res.get).with_args("/_all_dbs",
                headers={"Origin": origin}),
            raises(HTTPError)
        )
        db_get_resp = srv.res.get(db_path, headers={"Origin": origin})
        assert_that(
            db_get_resp,
            has_header("Access-Control-Allow-Origin", val=origin)
        )
        assert_that(
            db_get_resp,
            is_not(has_header("Access-Control-Allow-Credentials"))
        )
        assert_that(
            db_get_resp,
            has_header("Access-Control-Expose-Headers", val=EXPECTED_HEADERS)
        )
        assert_that(
            srv.res.post(db_path, headers={"Origin":origin}, data="{}").json(),
            has_key("ok")
        )
        # NOTE: This request would not work from the browser, as it's not
        # included in the allowed methods
        assert_that(
            srv.res.get(db_path+"/_all_docs", headers={"Origin":origin}).json(),
            has_key("rows")
        )

def test_no_cors():
    srv = cloudant.get_server(auth=(USER, USER))
    origin = "http://random-no-cors-domain.com"
    db_path = "/" + SHARED_DB
    with srv.user_context(USER, USER, owner=OWNER):
        assert_that(
            calling(srv.res.options).with_args(""),
            raises(HTTPError)
        )
        assert_that(
            calling(srv.res.options).with_args("", headers={"Origin": origin}),
            raises(HTTPError)
        )
        assert_that(
            srv.res.get("", headers={"Origin": origin}),
            is_not(has_header("Access-Control-Allow-Origin"))
        )
        assert_that(
            calling(srv.res.get).with_args("/_all_dbs",
                headers={"Origin": origin}),
            raises(HTTPError)
        )
        db_get_resp = srv.res.get(db_path, headers={"Origin": origin})
        assert_that(
            db_get_resp,
            is_not(has_header("Access-Control-Allow-Origin"))
        )
        assert_that(
            db_get_resp,
            is_not(has_header("Access-Control-Allow-Credentials"))
        )
        assert_that(
            db_get_resp,
            is_not(has_header("Access-Control-Expose-Headers"))
        )
        assert_that(
            srv.res.post(db_path, headers={"Origin":origin}, data="{}").json(),
            has_key("ok")
        )
        assert_that(
            srv.res.get(db_path+"/_all_docs", headers={"Origin":origin}).json(),
            has_key("rows")
        )

# COUCHDB-1689
def test_attachments():
    srv = cloudant.get_server(auth=(USER, USER))
    origin = "http://example.com"
    db_path = "/" + SHARED_DB
    text_doc_url = db_path + "/text_attachment_test"
    bin_doc_url = db_path + "/bin_attachment_test"
    with srv.user_context(USER, USER, owner=OWNER):
        text_put_resp = srv.res.put(
            text_doc_url+"/attachment.txt",
            headers={"Origin": origin, "Content-Type": "text/plain"},
            data="this is a text attachment"
        )
        assert_that(
            text_put_resp.status_code,
            is_(201)
        )
        assert_that(
            text_put_resp,
            has_header("Access-Control-Allow-Origin")
        )
        text_get_resp = srv.res.get(
            text_doc_url+"?attachments=true",
            headers={"Origin": origin}
        )
        assert_that(
            text_get_resp.status_code,
            is_(200)
        )
        assert_that(
            text_get_resp,
            has_header("Access-Control-Allow-Origin")
        )
        bin_put_resp = srv.res.put(
            bin_doc_url+"/attachment.bin",
            headers={
                "Origin": origin,
                "Content-Type": "application/octet-stream"
            },
            data="this is a binary attachment"
        )
        assert_that(
            bin_put_resp.status_code,
            is_(201)
        )
        assert_that(
            bin_put_resp,
            has_header("Access-Control-Allow-Origin")
        )
        bin_get_resp = srv.res.get(
            bin_doc_url+"/attachment.bin",
            headers={
                "Origin": origin,
                "Range": "bytes=0-6"
            }
        )
        assert_that(
            bin_get_resp.status_code,
            is_(206)
        )
        assert_that(
            bin_get_resp,
            has_header("Access-Control-Allow-Origin")
        )
        assert_that(
            bin_get_resp.content,
            is_("this is")
        )

def test_if_none_match_header():
    srv = cloudant.get_server(auth=(USER, USER))
    origin = "http://example.com"
    db_path = "/" + SHARED_DB
    doc_url = db_path + "/test_if_none_match"
    with srv.user_context(USER, USER, owner=OWNER):
        doc_put_resp = srv.res.put(
            doc_url,
            data="{}",
            headers={"Origin": origin}
        )
        assert_that(doc_put_resp.status_code, is_(201))
        etag = doc_put_resp.headers["ETag"]
        assert_that(
            srv.res.get(
                doc_url,
                headers={"Origin": origin, "If-None-Match": etag}
            ).status_code,
            is_(304))
