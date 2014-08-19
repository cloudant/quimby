
import functools
import uuid


import quimby.client


def setup_random_db(func):
    @functools.wraps(func)
    def decorator(test):
        """
        This should only be called on the setUp function of
        a unittest.TestCase
        """
        test.srv = quimby.client.get_server()
        test.res = test.srv.res
        test.db_name = "quimby_db_" + uuid.uuid4().hex
        test.db = test.srv.db(test.db_name)
        return f(test)
    return decorator


