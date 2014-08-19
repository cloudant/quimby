
from hamcrest import *
from hamcrest.core.base_matcher import BaseMatcher


is_ok = is_(200)
is_accepted = is_in([200, 201, 202])
is_bad_request = is_(400)
is_forbidden = is_(403)
is_not_found = is_(404)
is_precondition_failed = is_(412)


class HasHeader(BaseMatcher):
    def __init__(self, header, val=None):
        self.header = header
        self.val = val

    def _matches(self, resp):
        if self.val is None:
            return self.header in resp.headers
        else:
            return self.val == resp.headers.get(self.header)

    def describe_to(self, description):
        msg = "resp headers contains header {0}".format(self.header)
        description.append_text(msg)
        if self.val is not None:
            description.append_text(" with val {0}".format(self.val))

    def describe_mismatch(self, item, mismatch_description):
        if self.val is not None:
            msg = "got header value {0}".format(item.headers.get(self.header))
            mismatch_description.append_text(msg)
        else:
            msg = "headers are {0} ".format(item.headers)
            mismatch_description.append_text(msg)


def has_header(header, val=None):
    return HasHeader(header, val=val)
