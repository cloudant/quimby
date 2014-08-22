
import unittest
import uuid


import quimby.client


def random_db_name():
    return "quimby_db_" + uuid.uuid4().hex


class requires(object):
    def __init__(self, *args):
        self.args = args

    def __call__(self, obj):
        return unittest.skip("Not supported")(obj)


class DbPerClass(unittest.TestCase):
    Q = 1
    N = 3

    @classmethod
    def setUpClass(klass):
        klass.srv = quimby.client.default_server()
        klass.db = klass.srv.db(random_db_name())
        klass.db.create(q=klass.Q, n=klass.N)
        klass.res = klass.srv.res

    def setUp(self):
        self.srv = self.__class__.srv
        self.db = self.__class__.db
        self.res = self.__class__.res


class DbPerTest(unittest.TestCase):
    Q = 1
    N = 3

    def setUp(self):
        self.srv = quimby.client.default_server()
        self.db = self.srv.db(random_db_name())
        self.db.create(q=self.Q, n=self.N)
        self.res = self.srv.res
