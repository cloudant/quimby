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
