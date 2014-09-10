from hamcrest import *
from hamcrest.core.core.raises import *
from requests.exceptions import HTTPError

import cloudant
from cloudant import has_header

USER = "cors-foo"
TEST_DB = "test_cors_show_list_db"
ORIGIN = "https://foo.com"

CORS_CONFIG = {
    "cors": {
        "enable_cors": True,
        "allow_credentials": True,
        "origins": {
            "*": {}
        }
    }
}

DDOC = {
    "_id": "_design/foo",
    "views": {
        "noop": {
            "map": "function(doc) {}"
        }
    },
    "shows": {
        "bar": "function(doc, req) {return '<h1>wosh</h1>';}"
    },
    "lists": {
        "baz": "function(head, req) { provides('html', function() { return '<h1>vcxz</h1>'; }); }"
    },
    "updates": {
        "noop": "function(doc, req) { return [null, 'nada']; }"
    }
}


def setup_module():
    srv = cloudant.get_server()
    if not srv.user_exists(USER):
        srv.user_create(USER, USER, "foo@bar.com")
    assert(srv.user_config_set(USER, CORS_CONFIG))
    reset_db(srv)


def add_ddoc(db):
    resp = db.doc_save(DDOC)
    return resp


def reset_db(srv):
    with srv.user_context(USER, USER):
        db = srv.db(TEST_DB)
        db.reset()
        add_ddoc(db)


def test_shows():
    srv = cloudant.get_server(auth=(USER, USER))

    headers = {
        "Origin": ORIGIN,
        "Access-Control-Request-Method": "GET"
    }
    db_path = "/{}/_design/foo/_show/bar".format(TEST_DB)

    with srv.user_context(USER, USER):
        db_resp = srv.res.options(db_path, headers=headers)
        assert_that(
            db_resp.headers,
            has_key("access-control-allow-origin")
        )

        db_resp = srv.res.get(db_path, headers=headers)
        assert_that(
            db_resp.headers,
            has_key("access-control-allow-origin")
        )


def test_lists():
    srv = cloudant.get_server(auth=(USER, USER))

    headers = {
        "Origin": ORIGIN,
        "Access-Control-Request-Method": "GET"
    }
    db_path = "/{}/_design/foo/_list/baz/noop".format(TEST_DB)

    with srv.user_context(USER, USER):
        db_resp = srv.res.options(db_path, headers=headers)
        assert_that(
            db_resp.headers,
            has_key("access-control-allow-origin")
        )

        db_resp = srv.res.get(db_path, headers=headers)
        assert_that(
            db_resp.headers,
            has_key("access-control-allow-origin")
        )


def test_updates():
    srv = cloudant.get_server(auth=(USER, USER))

    headers = {
        "Origin": ORIGIN,
        "Access-Control-Request-Method": "POST"
    }
    db_path = "/{}/_design/foo/_update/noop".format(TEST_DB)

    with srv.user_context(USER, USER):
        db_resp = srv.res.options(db_path, headers=headers)
        assert_that(
            db_resp.headers,
            has_key("access-control-allow-origin")
        )

        db_resp = srv.res.post(db_path, headers=headers)
        assert_that(
            db_resp.headers,
            has_key("access-control-allow-origin")
        )
