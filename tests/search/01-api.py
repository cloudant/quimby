
from hamcrest import assert_that, has_key, has_length
from quimby.util.matchers import is_accepted
from quimby.util.test import DbPerClass, requires

import quimby.data as data


NUM_DOCS = 25
DDOC = {
    "_id": "_design/search",
    "indexes": {
        "text": {
            "index": "function(doc) {index(doc.val);}"
        }
    }
}


@requires("search")
class SearchAPITests(DbPerClass):

    def __init__(self, *args, **kwargs):
        super(SearchAPITests, self).__init__(*args, **kwargs)
        self.db.bulk_docs(data.gen_docs(count=NUM_DOCS, value="foo"))
        self.db.doc_save(DDOC)

    def test_no_results(self):
        r = self.search("notfound")
        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_key("results"))
        assert_that(r.json()["results"], has_length(0))

    def test_results(self):
        r = self.search("foo")
        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_key("results"))
        assert_that(r.json()["results"], has_length(NUM_DOCS))

    def test_limit(self):
        r = self.search("foo", limit=1)
        assert_that(r.status_code, is_accepted)
        assert_that(r.json(), has_key("results"))
        assert_that(r.json()["results"], has_length(1))

    def search(self, query, limit=None):
        params = {"q": query}
        if limit is not None:
            params["limit"] = limit
        path = self.path("_design", "search", "_view", "text")
        with self.res.return_errors():
            return self.res.get(path, params=params)
