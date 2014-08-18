
import base64
import contextlib as ctx
import hashlib
import json
import logging
import os
import random
import re
import struct
import time
import urllib
import urlparse
import StringIO


import requests



# Disable logging from requests
logging.getLogger("requests").setLevel(logging.WARNING)


DBNAME_PAT = r"shards/[a-fA-F0-9]{8}-[a-fA-F0-9]{8}/([^.]+)"
DBNAME_RE = re.compile(DBNAME_PAT)
HAPROXY_PORT = os.environ.get("HAPROXY_PORT", "5984")


def dbcopy_docid(key):
    if not isinstance(key, basestring):
        raise ValueError("only strings are currently supported")
    # First two bytes are 131, 109 for the external term format
    external = "\x83m" + struct.pack(">I", len(key)) + str(key)
    md5sum = hashlib.md5(external).digest()
    b64 = base64.b64encode(md5sum).rstrip("=")
    return b64.replace("/", "_").replace("+", "-")


def quote(str):
    return urllib.quote(str, safe="")


def parse_shard_name(name):
    match = DBNAME_RE.match(name)
    if not match:
        return name
    return match.group(1)


class EnvironmentConfig(object):

    DEFAULTS = {
        "TESTY_PROTOCOL": "http",
        "TESTY_CLUSTER": None,
        "TESTY_CLUSTER_LB": None,
        "TESTY_CLUSTER_NODENAMES": None,
        "TESTY_CLUSTER_NETLOC": "127.0.0.1:{0}".format(HAPROXY_PORT),
        "TESTY_NODE_NAMES": ",".join([
            "node1@127.0.0.1",
            "node2@127.0.0.1",
            "node3@127.0.0.1"
        ]),
        "TESTY_NODE_PUBLIC_INTERFACES": ",".join([
            "127.0.0.1:15984",
            "127.0.0.1:25984",
            "127.0.0.1:35984"
        ]),
        "TESTY_NODE_PRIVATE_INTERFACES": ",".join([
            "127.0.0.1:15986",
            "127.0.0.1:25986",
            "127.0.0.1:35986"
        ]),
        "TESTY_DB_ADMIN_USER": "adm",
        "TESTY_DB_ADMIN_PASS": "pass",
        "TESTY_DB_ADMIN_ROOT": None,
        "TESTY_DB_WRITE_USER": None,
        "TESTY_DB_WRITE_PASS": None,
        "TESTY_DB_READ_USER": None,
        "TESTY_DB_READ_PASS": None,
        "TESTY_DB_URL": None,
        "TESTY_DB_NAME": None,
        "TESTY_SAVE_URL": None,
        "TESTY_SAVE_USER": None,
        "TESTY_SAVE_PASS": None,
        "TESTY_RESULT_DIR": None,
        "TESTY_TIMESTART": None
    }

    def __init__(self):
        self.cfg = self.DEFAULTS.copy()
        for k in self.cfg:
            if k in os.environ:
                self.cfg[k] = os.environ[k]

        # Set node information
        names = self.cfg.get("TESTY_NODE_NAMES")
        public = self.cfg.get("TESTY_NODE_PUBLIC_INTERFACES")
        private = self.cfg.get("TESTY_NODE_PRIVATE_INTERFACES")
        self.nodes = []

        for n in names.split(","):
            self.nodes.append({"name": n})

        if public is not None:
            if len(public.split(",")) != len(self.nodes):
                raise ValueError("Mismatched public interfaces")
            for i, n in enumerate(public.split(",")):
                self.nodes[i]["public"] = n

        if private is not None:
            if len(private.split(",")) != len(self.nodes):
                raise ValueError("Mismatched private interfaces")
            for i, n in enumerate(private.split(",")):
                self.nodes[i]["private"] = n

        nodes = {}
        for n in self.nodes:
            nodes[n["name"]] = n
        self.nodes = nodes

    def __getattr__(self, name):
        envname = "TESTY_%s" % name.upper()
        if envname not in self.cfg:
            fmt = "'%s' object has no attribute '%s'"
            raise AttributeError(fmt % (self.__class__.__name__, name))
        return self.cfg[envname]

    @property
    def cluster_url(self):
        parts = (self.protocol, "://", self.cluster_netloc)
        return "".join(parts)

    def get_user(self, name):
        if name == "admin":
            if self.db_admin_user is not None:
                return (self.db_admin_user, self.db_admin_pass)
            else:
                return None
        elif name == "write":
            if self.db_write_user is not None:
                return (self.db_write_user, self.db_write_pass)
            else:
                return None
        elif name == "read":
            if self.db_read_user is not None:
                return (self.db_read_user, self.db_read_pass)
            else:
                return None
        raise ValueError("No user info for '%s'" % name)


CONFIG = EnvironmentConfig()


class Resource(object):
    def __init__(self, scheme, netloc, auth=None, session=None):
        self.scheme = scheme
        self.netloc = netloc
        if session is None:
            session = requests.session()
            adapter = requests.adapters.HTTPAdapter(pool_maxsize=256)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
        self.s = session
        self.s.auth = auth
        self.s.headers.update({
            "Content-Type": "application/json"
        })
        self.check_status_code = True

    @ctx.contextmanager
    def return_errors(self):
        original = self.check_status_code
        self.check_status_code = False
        try:
            yield self
        finally:
            self.check_status_code = original

    def head(self, path, **kwargs):
        return self._req("head", path, kwargs)

    def get(self, path, **kwargs):
        return self._req("get", path, kwargs)

    def put(self, path, **kwargs):
        return self._req("put", path, kwargs)

    def post(self, path, **kwargs):
        return self._req("post", path, kwargs)

    def delete(self, path, **kwargs):
        return self._req("delete", path, kwargs)

    def options(self, path, **kwargs):
        return self._req("options", path, kwargs)

    def _req(self, method, path, kwargs):
        url = self._url(path)
        self.last_req = getattr(self.s, method)(url, **kwargs)
        if self.check_status_code:
            self.last_req.raise_for_status()
        return self.last_req

    def _url(self, path):
        parts = (self.scheme, self.netloc, path, "", "")
        return urlparse.urlunsplit(parts)


class Server(object):
    def __init__(self, url, auth=None):
        parts = urlparse.urlsplit(url, "http", False)
        self.scheme = parts[0]
        self.netloc = parts[1]
        if parts[2] or parts[3] or parts[4]:
            raise ValueError("Invalid server URL: %s" % url)
        self.res = Resource(self.scheme, self.netloc, auth=auth)

    def welcome(self):
        r = self.res.get("")
        return r.json()

    def active_tasks(self):
        return self.res.get("_active_tasks").json()

    def all_dbs(self):
        return self.res.get("_all_dbs").json()

    def db(self, name):
        return Database(self, name)

    def global_changes(self, **kwargs):
        is_continuous = kwargs.get("feed") == "continuous"
        params = self._params(kwargs)
        r = self.res.get("_db_updates", params=params, stream=True)
        return Changes(self, r, is_continuous)

    def config_delete(self, section, key, persist=False):
        path = "/".join(["_config", section, key])
        hdrs = {"X-Couch-Persist": json.dumps(persist)}
        return self.res.delete(path, headers=hdrs).json()

    def config_get(self, section=None, key=None):
        if section is None and key is not None:
            raise ValueError("Unable to get key without a section")
        parts = ["_config"]
        if section is not None:
            parts.append(section)
        if key is not None:
            parts.append(key)
        path = "/".join(parts)
        return self.res.get(path).json()

    def config_set(self, section, key, value, persist=False):
        path = "/".join(["_config", section, key])
        hdrs = {"X-Couch-Persist": json.dumps(persist)}
        data = json.dumps(value)
        return self.res.put(path, headers=hdrs, data=data)

    def user_config_get(self, username):
        db = self.db("_users")
        if not db.exists():
            db.create()
        with db.srv.res.return_errors():
            user = db.doc_open(username)
            return user.get('config', {})

    def user_config_set(self, username, config):
        db = self.db("_users")
        if not db.exists():
            db.create()
        with db.srv.res.return_errors():
            user = db.doc_open(username)
            if config == user.get('config', {}):
                return True
            else:
                user['config'] = config
                resp = self.res.put(
                    "_users/{0}".format(username),
                    data=json.dumps(user)
                )
                return 200 <= self.res.last_req.status_code < 300

    def user_create(self, username, password, email, roles=None):
        db = self.db("_users")
        if not db.exists():
            db.create()
        user = {
            "_id": "org.couchdb.user:%s" % username,
            "name": username,
            "type": "user",
            "roles": [],
            "password": password
        }
        if roles is not None:
            for r in roles:
                if not isinstance(r, basestring):
                    raise TypeError("'%r' is not a string" % r)
                user["roles"].append(r)
        return self.res.put("_users/" + user["_id"], data=json.dumps(user))

    def user_exists(self, username):
        db = self.db("_users")
        with db.srv.res.return_errors():
            db.doc_open("org.couchdb.user:" + username)
            return 200 <= self.res.last_req.status_code < 300

    @ctx.contextmanager
    def user_context(self, username, password):
        orig_auth = self.res.s.auth
        try:
            self.res.s.auth = (username, password)
            yield
        finally:
            self.res.s.auth = orig_auth

    def wait_for_indexers(self, dbname=None, design_doc=None,
            min_delay=0.5, delay=0.25, max_delay=30.0):
        if design_doc is not None and not design_doc.startswith("_design/"):
            design_doc = "_design/" + design_doc
        def _match(t):
            if t.get("type") != "indexer":
                return False
            if parse_shard_name(t.get("database", "")) != dbname:
                return False
            if design_doc is not None:
                if t.get("design_document") != design_doc:
                    return False
            return True
        start = time.time()
        if min_delay is not None:
            time.sleep(min_delay)
        while any(_match(t) for t in self.active_tasks()):
            if time.time() - start > max_delay:
                raise RuntimError("Timeout waiting for indexer tasks")
            time.sleep(1.0)

    def last_status_code(self):
        return self.res.last_req.status_code

    def last_headers(self):
        return self.res.last_req.headers

    def _params(self, kwargs):
        ret = {}
        for k, v in kwargs.items():
            if not isinstance(v, basestring):
                v = json.dumps(v)
            ret[k] = v
        return ret


class Database(object):
    def __init__(self, server, name):
        self.srv = server
        if "/" in name:
            raise ValueError("Invalid database name: %s" % name)
        self.name = name

    @staticmethod
    def from_url(url):
        parts = urlparse.urlsplit(url, "http", False)
        if parts[3] or parts[4]:
            raise ValueError("Invalid databsae URL: %s" % url)
        srvurl = urlparse.urlunsplit(parts[:2] + ("", "", ""))
        srv = Server(srvurl)
        return srv.db(quote(parts.path.lstrip("/")))

    def path(self, *args):
        return "/".join((self.name,) + args)

    def exists(self, **kwargs):
        params = self._params(kwargs)
        with self.srv.res.return_errors() as res:
            r = res.head(self.name, params=params)
            return r.ok

    def create(self, **kwargs):
        params = self._params(kwargs)
        return self.srv.res.put(self.name, params=params)

    def delete(self, **kwargs):
        params = self._params(kwargs)
        return self.srv.res.delete(self.name, params=params)

    def reset(self, **kwargs):
        # Theoretically if we ever implement the TRUNCATE
        # command this would become a lot more efficient.
        params = self._params(kwargs)
        try:
            self.srv.res.delete(self.name, params=params)
        except:
            pass
        self.srv.res.put(self.name, params=params)

    def info(self, **kwargs):
        params = self._params(kwargs)
        return self.srv.res.get(self.name, params=params).json()

    def compact(self, wait=False, **kwargs):
        params = self._params(kwargs)
        r = self.srv.res.post(self.path("_compact"), params=params)
        if not wait:
            return r
        while self.info()["compact_running"]:
            pass

    def all_docs(self, **kwargs):
        return self._exec_view(self.path("_all_docs"), **kwargs)

    def doc_delete(self, doc_or_docid, **kwargs):
        if isinstance(doc_or_docid, basestring) and "rev" not in kwargs:
            docid = doc_or_docid
            rev = self.doc_open(docid)["_rev"]
        elif isinstance(doc_or_docid, basestring):
            docid = doc_or_docid
            rev = kwargs["rev"]
        else:
            docid = doc_or_docid["_id"]
            rev = doc_or_docid["_rev"]
        params = self._params(kwargs)
        params["rev"] = rev
        ret = self.srv.res.delete(self.path(docid), params=params).json()
        if isinstance(doc_or_docid, basestring):
            return ret
        else:
            doc_or_docid["_rev"] = ret["rev"]
            return doc_or_docid

    def doc_exists(self, docid, **kwargs):
        params = self._params(kwargs)
        with self.srv.res.return_errors() as res:
            r = res.head(self.path(docid), params=params)
            return r.ok

    def doc_open(self, docid, **kwargs):
        params = self._params(kwargs)
        return self.srv.res.get(self.path(docid), params=params).json()

    def doc_save(self, doc, **kwargs):
        if "_id" not in doc:
            path = self.path()
            func = self.srv.res.post
        else:
            path = self.path(quote(doc["_id"]))
            func = self.srv.res.put
        params = self._params(kwargs)
        r = func(path, data=json.dumps(doc), params=params)
        doc["_id"] = r.json()["id"]
        doc["_rev"] = r.json()["rev"]
        return doc

    def bulk_docs(self, docs, **kwargs):
        params = self._params(kwargs)
        data = json.dumps({"docs": docs})
        r = self.srv.res.post(self.path("_bulk_docs"), params=params, data=data)
        ret = r.json()
        for idx, result in enumerate(ret):
            if "error" in result:
                continue
            docs[idx]["_id"] = result["id"]
            docs[idx]["_rev"] = result["rev"]
        return ret

    def view(self, ddoc, vname, **kwargs):
        path = self.path("_design", ddoc, "_view", vname)
        return self._exec_view(path, **kwargs)

    def changes(self, **kwargs):
        is_continuous = kwargs.get("feed") == "continuous"
        params = self._params(kwargs)
        r = self.srv.res.get(self.path("_changes"), params=params, stream=True)
        return Changes(self, r, is_continuous)

    def wait_for_indexers(self, **kwargs):
        self.srv.wait_for_indexers(dbname=self.name, **kwargs)

    def wait_for_change(self, since, timeout=5000):
        c = self.changes(
                feed="longpoll",
                limit=1,
                since=since,
                timeout=timeout
            )
        if not len(c.results):
            raise RuntimeError("No change")
        return c.last_seq

    def last_status_code(self):
        return self.srv.last_status_code()

    def last_headers(self):
        return self.srv.last_headers()

    def get_security(self):
        return self.srv.res.get(self.path("_security")).json()

    def set_security(self, props):
        curr = self.get_security()
        if curr != props:
            with self.srv.res.return_errors() as res:
                res.put(self.path("_security"), data=json.dumps(props))
                return True
        else:
            return True

    def _exec_view(self, path, **kwargs):
        data = None
        func = self.srv.res.get
        if "keys" in kwargs:
            data = json.dumps({"keys":kwargs.pop("keys")})
            func = self.srv.res.post
        params = self._params(kwargs)
        r = func(path, data=data, params=params, stream=True)
        return ViewResult(self, r)

    def _params(self, kwargs):
        ret = {}
        for k, v in kwargs.items():
            if k in ("key", "startkey", "start_key", "endkey", "end_key"):
                ret[k] = json.dumps(v)
            elif not isinstance(v, basestring):
                ret[k] = json.dumps(v)
            else:
                ret[k] = v
        return ret


class ViewResult(object):
    def __init__(self, db, resp):
        self.db = db
        self.resp = resp
        result = self.resp.json()
        self.total_rows = result.get("total_rows")
        self.offset = result.get("offset")
        self.rows = result["rows"]

    def __iter__(self):
        return iter(self.rows)


class Changes(object):
    def __init__(self, db, resp, is_continuous):
        self.db = db
        self.resp = resp
        if is_continuous:
            self._gen = self._continuous
        else:
            self._gen = self._non_continuous
        self._running = False
        self._exhausted = False
        self._results = None
        self._last_seq = None

    def __iter__(self):
        self._running = True
        try:
            for change in self._gen(self.resp.iter_lines()):
                yield change
        finally:
            self._running = False
            self._exhausted = True

    @property
    def results(self):
        if self._results is None:
            self.read()
        return self._results

    @property
    def last_seq(self):
        if self._last_seq is None:
            self.read()
        return self._last_seq

    def read(self):
        if self._running:
            raise RuntimeError("Error reading from streamed changes feed.")
        if self._exhausted:
            raise RuntimeError("Error reading from exhausted changes feed.")
        self._results = []
        i = iter(self)
        for change in i:
            if "last_seq" in change:
                self._last_seq = change["last_seq"]
                break
            else:
                self._results.append(change)
        assert self._last_seq is not None
        for change in i:
            raise ValueError("Invalid change after last_seq: %r" % change)

    def _continuous(self, line_iter):
        for line in line_iter:
            if not line.strip():
                continue
            yield json.loads(line)

    def _non_continuous(self, line_iter):
        state = "init"
        for line in line_iter:
            if not line.strip():
                continue
            if line.strip() == '{"results":[':
                state = "starting"
                break
            else:
                raise ValueError("Invalid changes line: %r" % line)
        if state != "starting":
            raise RuntimeError("Invalid changes feed state. Expected starting")
        for line in line_iter:
            if not line.strip():
                continue
            if line.strip() == "],":
                state = "last_seq"
                break
            else:
                yield json.loads(line.rstrip().rstrip(","))
        if state != "last_seq":
            raise RuntimeError("Invalid changes feed state. Expected last_seq")
        for line in line_iter:
            if not line.strip():
                continue
            if line.startswith('"last_seq"'):
                yield json.loads('{' + line)
                state = "finishing"
                break
            else:
                raise ValueError("Invalid list_seq line: %r" % line)
        if state != "finishing":
            yield json
        for line in line_iter:
            if not line.strip():
                continue
            raise ValueError("Invalid changes feed data: %r" % line)


class ViewIterator(object):
    def __init__(self, db, resp):
        self.db = db
        self.resp = resp
        self.iter = self.resp.iter_lines()

        line = self.iter.next().strip()
        if line.endswith("]}"):
            header = json.loads(line)
        else:
            assert line.rstrip().endswith("["), "Invalid view result: %s" % line
            header = json.loads(line + "]}")
        assert "rows" in header, "Invalid view result"

        self.total_rows = header.get("total_rows")
        self.offset = header.get("offset")
        self._rows = None

    @property
    def rows(self):
        self._read()
        return self._rows

    def __iter__(self):
        for line in self.iter:
            if line.strip() == "]}":
                return
            yield json.loads(line.rstrip(","))

    def _read(self):
        if self._rows == None:
            self._rows = []
            for row in self:
                self._rows.append(row)


def get_server(node=None, interface="private", user="admin", auth=None):
    if auth is None:
        auth = CONFIG.get_user(user)
    if node is None:
        url = CONFIG.cluster_url
    else:
        url = "".join((CONFIG.protocol, "://", CONFIG.nodes[node][interface]))
    return Server(url, auth=auth)


def random_node(interface="private", user="admin"):
    name = random.choice(CONFIG.nodes.keys())
    return get_server(node=name, interface=interface, user=user)


def nodes(interface="private", user="admin"):
    ret = []
    for name in CONFIG.nodes.keys():
        ret.append(get_server(node=name, interface=interface, user=user))
    return ret
