from hamcrest import *
from hamcrest.core.core.raises import *
from requests.exceptions import HTTPError

import cloudant
from cloudant import has_header

OWNER = "cors-foo"
USER = "cors-bar"
USERS = [OWNER, USER]
SHARED_DB = "test_cors_shared_db"
ORIGIN = "https://{}.cloudant.com".format(USER)

CORS_CONFIG = {
    OWNER: {
        "cors": {
            "enable_cors": True,
            "allow_credentials": True,
            "origins": {
                "http://example.com": {}
            }
        }
    }
}

UNSHARED_SECURITY_DOC = {
    "cloudant": {
        OWNER: [
            "_reader",
            "_writer",
            "_creator",
            "_admin"
        ]
    }
}

SHARED_SECURITY_DOC = {
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

def setup_module():
    srv = cloudant.get_server()
    for user in USERS:
        if not srv.user_exists(user):
            srv.user_create(user, user, "foo@bar.com")
        if user in CORS_CONFIG:
            assert(srv.user_config_set(user, CORS_CONFIG[user]))


def test_unshared_db():
    srv = cloudant.get_server(auth=(USER, USER))
    headers = {
        "Origin": ORIGIN,
        "Access-Control-Request-Method": "GET"
    }
    db_path = "/" + SHARED_DB

    with srv.user_context(OWNER, OWNER):
        db = srv.db(SHARED_DB)
        db.reset()
        db.set_security(UNSHARED_SECURITY_DOC)

    with srv.user_context(USER, USER, owner=OWNER):
        assert_that(
            calling(srv.res.options).with_args(db_path),
            raises(HTTPError)
        )
        assert_that(
            calling(srv.res.options).with_args(db_path, headers=headers),
            raises(HTTPError)
        )


def test_shared_db():
    srv = cloudant.get_server(auth=(USER, USER))
    headers = {
        "Origin": ORIGIN,
        "Access-Control-Request-Method": "GET"
    }
    db_path = "/" + SHARED_DB

    with srv.user_context(OWNER, OWNER):
        db = srv.db(SHARED_DB)
        db.reset()
        db.set_security(SHARED_SECURITY_DOC)

    with srv.user_context(USER, USER, owner=OWNER):
        db_resp = srv.res.get(db_path)
        assert_that(
            db_resp.json(),
            has_key("update_seq")
        )
        db_options_resp = srv.res.options(db_path, headers=headers)
        assert_that(
            db_options_resp,
            has_header("Access-Control-Allow-Origin", val=ORIGIN)
        )
        assert_that(
            db_options_resp,
            has_header("Access-Control-Allow-Credentials", val="true")
        )
        db_cors_resp = srv.res.options(db_path, headers=headers)
        assert_that(
            db_cors_resp,
            has_header("Access-Control-Allow-Origin", val=ORIGIN)
        )
        assert_that(
            db_cors_resp,
            has_header("Access-Control-Allow-Credentials", val="true")
        )

