import textwrap
import uuid

from hamcrest import assert_that, is_
from quimby.util.test import DbPerTest


class MultiPartStatusCodeTests(DbPerTest):

    def test_trailing_crlf(self):
        body = textwrap.dedent("""\
            --{0}\r
            Content-Type: application/json\r
            \r
            {{
                "_attachments": {{
                    "ohai": {{
                        "follows": true,
                        "content_type": "text/plain",
                        "length": 4
                    }}
                }}
            }}\r
            --{0}\r
            \r
            ohai\r
            --{0}--\r\n""")
        self.run(body, 201)

    def test_missing_cr_after_content_type(self):
        body = textwrap.dedent("""\
            --{0}\r
            Content-Type: application/json\r
            \n\
            {{
                "_attachments": {{
                    "ohai": {{
                        "follows": true,
                        "content_type": "text/plain",
                        "length": 4
                    }}
                }}
            }}\r
            --{0}\r
            \r
            ohai\r
            --{0}--""")
        self.run(body, 400)

    def test_spurious_lf_in_part(self):
        body = textwrap.dedent("""\
            --{0}\r
            Content-Type: application/json\r
            \r
            {{
                "_attachments": {{
                    "ohai": {{
                        "follows": true,
                        "content_type": "text/plain",
                        "length": 4
                    }}
                }}
            }}\r
            --{0}\r
            \r
            ohai\r\n
            --{0}--""")
        self.run(body, 400)

    def test_extra_lf_after_content_type(self):
        body = textwrap.dedent("""\
            --{0}\r
            Content-Type: application/json\r\n
            \r
            {{
                "_attachments": {{
                    "ohai": {{
                        "follows": true,
                        "content_type": "text/plain",
                        "length": 4
                    }}
                }}
            }}\r
            --{0}\r
            \r
            ohai\r
            --{0}--""")
        self.run(body, 400)

    def test_missing_cr_after_second_boundary(self):
        body = textwrap.dedent("""\
            --{0}\r
            Content-Type: application/json\r
            \n\
            {{
                "_attachments": {{
                    "ohai": {{
                        "follows": true,
                        "content_type": "text/plain",
                        "length": 4
                    }}
                }}
            }}\r
            --{0}\r
            \r
            ohai\r
            --{0}--""")
        self.run(body, 400)

    def test_part_too_short(self):
        body = textwrap.dedent("""
            --{0}\r
            Content-Type: application/json\r
            \r
            {{
                "_attachments": {{
                    "ohai": {{
                        "follows": true,
                        "content_type": "text/plain",
                        "length": 4
                    }}
                }}
            }}\r
            --{0}\r
            \r
            oha\r
            --{0}--""")
        self.run(body, 400)

    def test_part_too_long(self):
        body = textwrap.dedent("""\
            --{0}\r
            Content-Type: application/json\r
            \r
            {{
                "_attachments": {{
                    "ohai": {{
                        "follows": true,
                        "content_type": "text/plain",
                        "length": 4
                    }}
                }}
            }}\r
            --{0}\r
            \r
            ohaii\r
            --{0}--""")
        self.run(body, 400)

    def test_bad_content_type(self):
        body = textwrap.dedent("""\
            --{0}\r
            Content-Type: application/wat\r
            \r
            {{
                "_attachments": {{
                    "ohai": {{
                        "follows": true,
                        "content_type": "text/plain",
                        "length": 4
                    }}
                }}
            }}\r
            --{0}\r
            \r
            ohai\r
            --{0}--""")
        self.run(body, 415)

    def test_bad_first_boundary(self):
        body = textwrap.dedent("""\
            -{0}\r
            Content-Type: application/json\r
            \r
            {{
                "_attachments": {{
                    "ohai": {{
                        "follows":true,
                        "content_type": "text/plain",
                        "length": 4
                    }}
                }}
            }}\r
            --{0}\r
            \r
            ohai\r
            --{0}--""")
        self.run(body, 415)

    def run(self, body, code):
        boundary = uuid.uuid4().hex
        hdrs = {
            'Content-Type': 'multipart/related;boundary="{0}"'.format(boundary)
        }
        body = body.format(boundary)
        with self.res.return_errors():
            r = self.res.put(self.db.path("testdoc"), headers=hdrs, data=body)
        assert_that(r.status_code, is_(code))
