#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.


import optparse as op
import os
import traceback
import types
import unittest


from quimby.util.test import DbPerClass, DbPerTest

AUTHOR = "Cloudant, an IBM Company"
USAGE = "%prog [OPTIONS] PATH1 [PATH2 ...]"


def tests_from_file(fname):
    env = {}
    try:
        execfile(fname, env)
    except:
        traceback.print_exc()
        exit(1)
    for k, v in env.items():
        if type(v) is not type:
            continue
        if v in (DbPerClass, DbPerTest):
            continue
        if issubclass(v, unittest.TestCase):
            print k, v
            yield v


def load_tests(path_list):
    seen = set()
    suites = []
    loader = unittest.TestLoader()
    for root in path_list:
        for path, dnames, fnames in os.walk(root):
            for fname in fnames:
                if not fname.endswith(".py"):
                    continue
                fname = os.path.abspath(os.path.join(path, fname))
                if fname in seen:
                    continue
                seen.add(fname)
                for test in tests_from_file(fname):
                    suites.append(loader.loadTestsFromTestCase(test))
    return suites


def options():
    return [
        op.make_option(
            '-a', '--admin',
            metavar='USER:PASS',
            help='Username/password pair for the admin user.'
        )
    ]


def main():
    parser = op.OptionParser(usage=USAGE, option_list=options())
    opts, args = parser.parse_args()

    if not len(args):
        parser.error("No test paths specified.")

    for suite in load_tests(args):
        unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == '__main__':
    main()
