
import json
import logging
import os
import re
import time
import urllib
import urlparse


import requests


# Disable logging from requests
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)


DBNAME_PAT = r"shards/[a-fA-F0-9]{8}-[a-fA-F0-9]{8}/([^.]+)"
DBNAME_RE = re.compile(DBNAME_PAT)


def quote(str):
    return urllib.quote(str, safe="")


def _parse_dbname(name):
    match = DBNAME_RE.match(name)
    if not match:
        return name
    return match.group(1)


class Resource(object):
    def __init__(self, scheme, netloc, auth=None, session=None):
        self.scheme = scheme
        self.netloc = netloc
        if session is None:
            session = requests.session()
        self.s = session
        self.s.auth = auth
        self.s.headers.update({
            "Content-Type": "application/json"
        })
        self.check_status_code = True

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
            raise ValueError("Invalid server URL: {}".format(url))
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

    def wait_for_indexers(self, dbname=None, design_doc=None,
            min_delay=0.5, delay=0.25, max_delay=30.0):
        if design_doc is not None and not design_doc.startswith("_design/"):
            design_doc = "_design/" + design_doc
        def _match(t):
            if t.get("type") != "indexer":
                return False
            if _parse_dbname(t.get("database", "")) != dbname:
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


class Database(object):
    def __init__(self, server, name):
        self.srv = server
        if "/" in name:
            raise ValueError("Invalid database name: {}".format(name))
        self.name = name

    @staticmethod
    def from_url(url):
        parts = urlparse.urlsplit(url, "http", False)
        if parts[3] or parts[4]:
            raise ValueError("Invalid databsae URL: {}".format(url))
        srvurl = urlparse.urlunsplit(parts[:2] + ("", "", ""))
        srv = Server(srvurl)
        return srv.db(quote(parts.path.lstrip("/")))

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
        return self.srv.res.get(self.name, params=params)

    def all_docs(self, **kwargs):
        path = "/".join([self.name, "_all_docs"])
        return self._exec_view(path, **kwargs)

    def doc_delete(self, doc_or_docid, **kwargs):
        if isinstance(doc_or_docid, (str, unicode)) and "rev" not in kwargs:
            docid = doc_or_docid
            rev = self.doc_open(docid)["_rev"]
        elif isinstance(doc_or_docid, (str, unicode)):
            docid = doc_or_docid
            rev = kwargs["rev"]
        else:
            docid = doc_or_docid["_id"]
            rev = doc_or_docid["_rev"]
        path = "/".join((self.name, docid))
        params = self._params(kwargs)
        params["rev"] = rev
        return self.srv.res.delete(path, params=params).json()

    def doc_open(self, docid, **kwargs):
        path = "/".join(self.name, docid)
        params = self._params(kwargs)
        return self.srv.res.get(path, params=params).json()

    def doc_save(self, doc, **kwargs):
        if "_id" not in doc:
            path = self.name
            func = self.srv.res.post
        else:
            path = "/".join((self.name, quote(doc["_id"])))
            func = self.srv.res.put
        params = self._params(kwargs)
        r = func(path, data=json.dumps(doc), params=params)
        doc["_id"] = r.json()["id"]
        doc["_rev"] = r.json()["rev"]
        return doc

    def view(self, ddoc, vname, **kwargs):
        path = "/".join([self.name, "_design", ddoc, "_view", vname])
        return self._exec_view(path, **kwargs)

    def wait_for_indexers(self, **kwargs):
        self.srv.wait_for_indexers(dbname=self.name, **kwargs)

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


def default_server():
    admuser = os.getenv("TEST_DB_ADMIN_USER", "adm")
    admpass = os.getenv("TEST_DB_ADMIN_PASS", "pass")
    auth = (admuser, admpass)
    url = os.getenv("TESTY_DB_ADMIN_ROOT", "http://127.0.0.1:5984")
    return Server(url, auth=auth)
