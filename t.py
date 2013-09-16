
def eq(a, b, msg=None):
    if msg is None:
        assert a == b, "%r != %r" % (a, b)
    else:
        assert a == b, msg


def ne(a, b, msg=None):
    if msg is None:
        assert a != b, "%r == %r" % (a, b)
    else:
        assert a != b, msg


def lt(a, b, msg=None):
    if msg is None:
        assert a < b, "%r >= %r" % (a, b)
    else:
        assert a < b, msg


def gt(a, b, msg=None):
    if msg is None:
        assert a > b, "%r <= %r" % (a, b)
    else:
        assert a > b, msg


def isin(a, b, msg=None):
    if msg is None:
        assert a in b, "%r is not in %r" % (a, b)
    else:
        assert a in b, msg


def isnotin(a, b, msg=None):
    if msg is None:
        assert a not in b, "%r is in %r" % (a, b)
    else:
        assert a not in b, msg


def has(a, b, msg=None):
    if msg is None:
        assert hasattr(a, b), "%r has no attribute %r" % (a, b)
    else:
        assert hasattr(a, b), msg


def hasnot(a, b, msg=None):
    if msg is None:
        assert not hasattr(a, b), "%r has an attribute %r" % (a, b)
    else:
        assert not hasattr(a, b), msg


def istype(a, b, msg=None):
    if msg is None:
        assert isinstance(a, b), "%r is not an instance of %r" % (a, b)
    else:
        assert isinstance(a, b), msg


def raises(exctype, func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except exctype:
        return
    func_name = getattr(func, "func_name", "<builtin_function>")
    args = (func_name, exctype.__name__)
    raise AssertionError("Function %s did not raise %s" % args)
