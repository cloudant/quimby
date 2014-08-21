
import unittest
import uuid


import quimby.client


def random_db_name():
    return "quimby_db_" + uuid.uuid4().hex


class requires(object):
    def __init__(self, *args):
        self.args = args

    def __call__(self, obj):
        return obj


class DbPerClass(unittest.TestCase):
    Q = 1
    N = 3

    def __init__(self, *args, **kwargs):
        super(DbPerClass, self).__init__(*args, **kwargs)
        self.srv = quimby.client.default_server()
        self.db = self.srv.db(random_db_name())
        self.db.create(q=self.Q, n=self.N)
        self.res = self.srv.res


class DbPerTest(unittest.TestCase):
    Q = 1
    N = 3

    def setUp(self):
        super(DbPerTest, self).setUp()
        self.srv = quimby.client.get_server()
        self.db = self.srv.db(random_db_name())
        self.db.create(q=self.Q, n=self.N)
        self.res = self.srv.res
