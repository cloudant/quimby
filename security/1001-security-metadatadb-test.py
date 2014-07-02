from hamcrest import *
from hamcrest.core.core.raises import *
from requests.exceptions import HTTPError

import copy
import json
import time

import cloudant


CASSIM_DB = "cassim"
SHARED_DB = "test_security_metadata"
DB_PATH = "/" + SHARED_DB

OWNER = "foo"
USER = "bar"
USERS = [OWNER, USER]

SEC_DOC = {
    "cloudant": {
        "foo": ["_reader","_writer","_admin"]
    }
}

ROLES_LIST = [
    ["_reader"],
    ["_writer"],
    ["_admin"],
    ["_reader", "_writer"],
    ["_reader", "_writer", "_admin"]
]


def disable_cassim(srv):
    cdb = srv.db(CASSIM_DB)
    with cdb.srv.res.return_errors():
        cdb.delete()


def enable_cassim():
    srv = cloudant.get_server()
    cdb = srv.db(CASSIM_DB)
    cdb.reset()


def setup_module():
    srv = cloudant.get_server()

    for user in USERS:
        if not srv.user_exists(user):
            srv.user_create(user, user, "foo@bar.com")

    disable_cassim(srv)

    with srv.user_context(OWNER, OWNER):
        db = srv.db(SHARED_DB)
        db.reset()


def set_security(srv, roles=None):
    sec_doc = copy.deepcopy(SEC_DOC)
    if roles is not None:
        sec_doc["cloudant"]["bar"] = roles

    with srv.user_context(OWNER, OWNER):
        db = srv.db(SHARED_DB)
        db.set_security(sec_doc)


def check_roles(srv, roles=[]):
    # check _reader
    try:
        srv.res.get(DB_PATH).json().get("db_name")
        assert("_reader" in roles)
    except HTTPError:
        assert("_reader" not in roles)
    # check _writer
    try:
        srv.res.post(DB_PATH, data=json.dumps({"foo":"bar"}))
        assert("_writer" in roles)
    except HTTPError:
        assert("_writer" not in roles)
    # check _admin
    try:
        srv.res.get(DB_PATH + "/_security")
        assert("_admin" in roles)
    except HTTPError:
        assert("_admin" not in roles)


def assert_user_roles(srv, user, roles):
    set_security(srv, roles=roles)
    with srv.user_context(user, user, owner=OWNER):
        check_roles(srv, roles)


def assert_cassim_roles(srv, roles):
    suffix = current_db_suffix()
    url = "/cassim/{0}%2f{1}%2f_security{2}".format(OWNER, SHARED_DB, suffix)
    srv2 = cloudant.get_server()
    resp = srv2.res.get(url).json()
    bar_roles = resp["cloudant"].get("bar", [])
    assert(bar_roles == roles)


def current_db_suffix():
    srvp = cloudant.get_server(node="node1@127.0.0.1")
    url = "/dbs/{0}%2f{1}".format(OWNER, SHARED_DB)
    resp = srvp.res.get(url).json()
    return suffix_to_string(resp["shard_suffix"])


def suffix_to_string(suffix):
    return ''.join(map(chr, suffix))


# roles = []
def test_basic_security():
    srv = cloudant.get_server(auth=(USER,USER))
    assert_user_roles(srv, USER, ["_reader"])


# roles = ["_reader"]
def test_reader_shared_security():
    srv = cloudant.get_server(auth=(USER,USER))
    assert_user_roles(srv, USER, ["_reader"])


# roles = ["_writer"]
def test_writer_shared_security():
    srv = cloudant.get_server(auth=(USER,USER))
    assert_user_roles(srv, USER, ["_writer"])


# roles = ["_reader", "_writer"]
def test_reader_writer_shared_security():
    srv = cloudant.get_server(auth=(USER,USER))
    assert_user_roles(srv, USER, ["_reader", "_writer"])


# roles = ["_reader", "_writer", "_admin"]
def test_reader_writer_admin_shared_security():
    srv = cloudant.get_server(auth=(USER,USER))
    assert_user_roles(srv, USER, ["_reader", "_writer", "_admin"])


# roles = ["_admin"]
def test_admin_shared_security():
    srv = cloudant.get_server(auth=(USER,USER))
    assert_user_roles(srv, USER, ["_admin"])


# cassim tests

def test_cassim():
    enable_cassim()

    srv = cloudant.get_server(auth=(USER,USER))

    for roles in ROLES_LIST:
        # Give cassim a second to catch up
        time.sleep(1)
        # print "Testing cassim with roles: {}".format(roles)
        assert_user_roles(srv, USER, roles)
        assert_cassim_roles(roles)
