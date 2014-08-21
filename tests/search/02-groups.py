# import json
# import unittest
# import requests
# import testy.utils.http
# import testy.utils.search
#
# # Note: These tests are non-mutating. They would probably go much faster
# #       if they didn't have to set-up the database each time. However,
# #       that's awkward right now in Testy.
#
#
# class TestSearchGroupingApi(testy.utils.http.HttpTestCase):
#     """
#     Tests the HTTP search grouping API functions correctly.
#
#     N.B. the test data is a cross-product of 5 fish and 5 cakes.
#     """
#
#     FISH_COUNT = 5
#     CAKE_COUNT = 5
#     RECORD_COUNT = FISH_COUNT * CAKE_COUNT
#
#     def setUp(self):
#         super(TestSearchGroupingApi, self).setUp()
#         self._delete_db(fail_if_not_found=False)
#         self._create_db()
#         self._populate_db()
#
#     def tearDown(self):
#         self._delete_db()
#         super(TestSearchGroupingApi, self).tearDown()
#
#     def test_old_api_gets_expected_grouped_results(self):
#         """
#         Test the general structure and content of a matching search under the
#         old API.
#         """
#         response = self._search(q="fish:cod OR cake:[a* TO c*]",
#                                 group_by="cake")
#         self._assert_old_grouped_response(response,
#                                           group_count=self.CAKE_COUNT,
#                                           row_count=9, grouped_row_count=9)
#         group = self._get_first_group_by_id_alphabetical(response)
#         self._assert_old_group(group, row_count=self.FISH_COUNT,
#                                total_row_count=self.FISH_COUNT,
#                                by="battenburg")
#
#         # Check the hit structure and content
#         hit = group["hits"][0]
#         self._assert_matched_row_structure(hit)
#         # Hits are sorted by relevance, which means the id will be...
#         self.assertEqual("cod-battenburg", hit["id"])
#
#         self._assert_matched_row_fields(hit, cake="battenburg")
#
#     def test_old_api_gets_no_grouped_results_if_query_unmatched(self):
#         """
#         Test the old API response if the search is unmatched.
#
#         This test is interesting in a white-box sense, because it's a
#         distinct code path (it calls into dreyfus_fabric_search1 but not
#         dreyfus_fabric_search2).
#         """
#         response = self._search(q="fish:whale", group_by="cake")
#         self._assert_old_grouped_response(
#             response,
#             group_count=0,
#             row_count=0,
#             grouped_row_count=0
#         )
#
#     def test_grouped_search_gets_expected_grouped_results(self):
#         """
#         Test the general structure and content of a matching search.
#         """
#         response = self._search(q="fish:cod OR cake:[a* TO c*]",
#                                 group_field="cake")
#
#         self._assert_new_grouped_response(response,
#                                           group_count=self.CAKE_COUNT,
#                                           row_count=9)
#
#         # Check the group content
#         group = self._get_first_group_by_id_alphabetical(response)
#         self._assert_new_group(group, row_count=self.FISH_COUNT,
#                                total_row_count=self.FISH_COUNT,
#                                by="battenburg")
#
#         # Check the hit structure and content
#         hit = group["rows"][0]
#         self._assert_matched_row_structure(hit)
#         # Hits are sorted by relevance, which means the id will be...
#         self.assertEqual("cod-battenburg", hit["id"])
#
#         self._assert_matched_row_fields(hit, cake="battenburg")
#
#     def test_grouped_search_gets_no_results_if_query_unmatched(self):
#         """
#         Test the API response if the search is unmatched.
#
#         This test is interesting in a white-box sense, because it's a
#         distinct code path (it calls into dreyfus_fabric_search1 but not
#         dreyfus_fabric_search2).
#         """
#         response = self._search(q="fish:whale", group_field="cake")
#         self._assert_new_grouped_response(
#             response,
#             group_count=0,
#             row_count=0
#         )
#
#     def test_limit_parameter_limits_hit_count_in_each_group(self):
#         response = self._search(
#             q="fish:[a TO z]",
#             group_field="cake",
#             limit=2
#         )
#         groups = response["groups"]
#         self.assertEqual(self.CAKE_COUNT, len(groups),
#                          "Group count was incorrectly limited")
#         for group in groups:
#             self.assertEqual(2, len(group["rows"]),
#                              "%s group did not obey ?limit=2" % group["by"])
#
#     def test_group_limit_parameter_limits_group_count(self):
#         response = self._search(q="fish:[a TO z]", group_field="cake",
#                                 group_limit=2)
#         groups = response["groups"]
#         self.assertEqual(2, len(groups))
#         for group in groups:
#             self.assertEqual(self.FISH_COUNT, len(group["rows"]),
#                       "%s group was incorrectly limited" % group["by"])
#
#     def test_group_limit_parameter_has_no_effect_on_fewer_groups(self):
#         response = self._search(q="fish:[a TO z]", group_field="cake",
#                                 group_limit=10)
#         self.assertEqual(self.CAKE_COUNT, len(response["groups"]))
#
#     def test_old_api_total_grouped_hits_is_changed_by_results_limits(self):
#         """
#         Demonstrate that the old API's total_grouped_hits field responds to
#         the result limit parameters.
#
#         """
#         LIMIT = 3
#         GROUP_LIMIT = 4
#         response = self._search(q="fish:[a TO z]", group_by="cake",
#                                 limit=LIMIT, group_limit=GROUP_LIMIT)
#         self.assertEqual(GROUP_LIMIT * self.FISH_COUNT,
#                          response["total_grouped_hits"])
#
#     def test_total_hits_is_not_changed_by_results_limits(self):
#         response = self._search(
#             q="fish:[a TO z]", group_field="cake", limit=3,
#                                 group_limit=4)
#         self.assertEqual(self.RECORD_COUNT, response["total_rows"])
#         for group in response["groups"]:
#             self.assertEqual(self.FISH_COUNT, group["total_rows"])
#
#     def test_sort_parameter_determines_hit_order_within_groups(self):
#         response = self._search(q="fish:[a TO z]", group_field="cake",
#                                 sort='"-fish<string>"')
#         for group in response["groups"]:
#             self._assert_doc_ids_in_group_startwith(group, ["tuna-",
#                                                             "shark-",
#                                                             "haddock-",
#                                                             "cod-",
#                                                             "angler-"])
#
#     def test_sort_parameter_does_not_change_group_order(self):
#         response = self._search(
#             q="fish:cod OR cake:fairy", group_field="cake",
#                                 sort='"-cake<string>"')
#         # Default sort order is relevance, so "fairy" should be the top group
#         # (since it appears in the query and the groups are using default
#         # sort order).
#         self.assertEqual("fairy", response["groups"][0]["by"])
#
#     def test_array_sort_parameter_sorts_by_left_criterion_last(self):
#         # Sort according to whether the first letter is in the first half of
#         # the alphabet, then reverse alphabetically.
#         response = self._search(q="fish:[a TO z]", group_field="cake",
#                                 sort='["-fish_in_h1", "-fish<string>"]')
#         group = response["groups"][0]  # All groups are the same; choose any.
#         self._assert_doc_ids_in_group_startwith(group, ["haddock-",
#                                                         "cod-",
#                                                         "angler-",
#                                                         "tuna-",
#                                                         "shark-"])
#
#     def test_group_sort_parameter_determines_group_order(self):
#         response = self._search(q="fish:cod", group_field="cake",
#                                 group_sort='"-cake<string>"')
#         groups = response["groups"]
#         self._assert_group_names(groups, ["victoria sandwich",
#                                           "rock",
#                                           "fairy",
#                                           "cup",
#                                           "battenburg"])
#
#     def test_group_sort_parameter_does_not_change_hit_order(self):
#         response = self._search(q="fish:cod OR fish:[r TO z]",
#                                 group_field="cake",
#                                 group_sort='"-fish<string>"')
#         for group in response["groups"]:
#             top_hit = group["rows"][0]
#             self.assertTrue(top_hit["id"].startswith("cod-"),
#                             "?group_sort= incorrectly changed hit order")
#
#     def test_array_group_sort_parameter_sorts_by_left_criteron_last(self):
#         # Sort groups according to whether the first letter is in the second
#         # half of the alphabet, then alphabetically.
#         response = self._search(q="fish:[a TO z]", group_field="cake",
#                                 group_sort='["cake_in_h1", "cake<string>"]')
#         groups = response["groups"]
#         self._assert_group_names(groups, ["rock",
#                                           "victoria sandwich",
#                                           "battenburg",
#                                           "cup",
#                                           "fairy"])
#
#     def test_sorting_occurs_before_limiting_on_grouped_rows(self):
#         response = self._search(q="fish:[a TO z]", group_field="cake",
#                                 limit=2, sort='"-fish<string>"')
#         group = response["groups"][0]
#         self._assert_doc_ids_in_group_startwith(group, ["tuna-",
#                                                         "shark-"])
#
#     def test_sorting_occurs_before_limiting_on_groups(self):
#         response = self._search(q="fish:[a TO z]", group_field="cake",
#                                 group_limit=2, group_sort='"-cake<string>"')
#         self._assert_group_names(response["groups"], ["victoria sandwich",
#                                                       "rock"])
#
#     def _assert_old_grouped_response(self, response, group_count, row_count,
#                                      grouped_row_count):
#         self.assertTrue("groups" in response)
#         self.assertTrue("total_grouped_hits" in response)
#         self.assertTrue("total_hits" in response)
#         self.assertEqual(row_count, response["total_hits"])
#         self.assertEqual(grouped_row_count, response["total_grouped_hits"])
#         self.assertEqual(group_count, len(response["groups"]))
#         if group_count > 0:
#             self._assert_old_group_structure(response["groups"][0])
#
#     def _assert_old_group_structure(self, group):
#         self.assertTrue("hits" in group)
#         self.assertTrue("total_hits" in group)
#         self.assertTrue("by" in group)
#
#     def _assert_old_group(self, group, row_count, total_row_count, by):
#         self._assert_old_group_structure(group)
#         self.assertEqual(row_count, len(group["hits"]))
#         self.assertEqual(total_row_count, group["total_hits"])
#         self.assertEqual(by, group["by"])
#
#     def _assert_new_grouped_response(self, response, group_count, row_count):
#         self.assertTrue("groups" in response)
#         self.assertTrue("total_rows" in response)
#         # There is no analogue to total_grouped_hits in the new API.
#         self.assertTrue("total_grouped_hits" not in response)
#         self.assertTrue("total_grouped_rows" not in response)
#         self.assertEqual(row_count, response["total_rows"])
#         self.assertEqual(group_count, len(response["groups"]))
#         if group_count > 0:
#             self._assert_new_group_structure(response["groups"][0])
#
#     def _assert_new_group_structure(self, group):
#         self.assertTrue("rows" in group)
#         self.assertTrue("total_rows" in group)
#         self.assertTrue("by" in group)
#
#     def _assert_new_group(self, group, row_count, total_row_count, by):
#         self._assert_new_group_structure(group)
#         self.assertEqual(row_count, len(group["rows"]))
#         self.assertEqual(total_row_count, group["total_rows"])
#         self.assertEqual(by, group["by"])
#
#     def _get_first_group_by_id_alphabetical(self, response):
#         return sorted(response["groups"], key=lambda x: x["by"])[0]
#
#     def _assert_matched_row_structure(self, row):
#         self.assertTrue("fields" in row)
#         self.assertTrue("order" in row)
#         self.assertTrue("id" in row)
#
#     def _assert_matched_row_fields(self, row, **kwargs):
#         fields = row["fields"]
#         for key, value in kwargs.items():
#             self.assertTrue(key in fields)
#             self.assertEqual("battenburg", fields[key])
#
#     def _assert_doc_ids_in_group_startwith(self, group, prefixes):
#         self._assert_new_group_structure(group)
#         doc_ids = [row["id"] for row in group["rows"]]
#         self.assertEqual(len(prefixes), len(doc_ids))
#         for actual_id, expected_prefix in zip(doc_ids, prefixes):
#             self.assertTrue(actual_id.startswith(expected_prefix),
#                             "%s does not being with %s" % (actual_id,
#                                                            expected_prefix))
#
#     def _assert_group_names(self, groups, names):
#         self.assertEqual(len(names), len(groups))
#         for group, expected_name in zip(groups, names):
#             actual_name = group["by"]
#             self.assertEqual(expected_name, actual_name)
#
#     @property
#     def db_url(self):
#         return self._env['TESTY_DB_URL']
#
#     @property
#     def ddoc_path(self):
#         return '_design/ddoc'
#
#     @property
#     def ddoc_url(self):
#         return '/'.join((self.db_url, self.ddoc_path))
#
#     @property
#     def search_path(self):
#         return '{0}/_search/s'.format(self.ddoc_path)
#
#     @property
#     def search_url(self):
#         return '/'.join((self.db_url, self.search_path))
#
#     def _create_db(self):
#         self.put(self.db_url)
#
#     def _delete_db(self, fail_if_not_found=True):
#         self.delete(self.db_url, do_raise=fail_if_not_found)
#
#     def _populate_db(self):
#         # Documents
#         fish = ['cod', 'haddock', 'angler', 'shark', 'tuna']
#         self.assertEqual(self.FISH_COUNT, len(fish))
#         cakes = ['fairy', 'cup', 'rock', 'battenburg', 'victoria sandwich']
#         self.assertEqual(self.CAKE_COUNT, len(cakes))
#         for fish, cake in ((f, c) for f in fish for c in cakes):
#             doc = {'fish': fish, 'cake': cake}
#             doc_id = '%s-%s' % (fish, cake)
#             self.put(self.db_url + '/' + doc_id,
#                      headers={'Content-Type': 'application/json'},
#                      data=json.dumps(doc))
#
#         # Design doc
#         index_function = """
#         function(doc) {
#             index("default", doc._id);
#             if (doc.fish) {
#                 index("fish", doc.fish, {"index": "not_analyzed"});
#                 index("fish_in_h1", doc.fish < "n" ? 1 : 0);
#             }
#             if (doc.cake) {
#                 index("cake", doc.cake, {"store": "yes",
#                                          "index": "not_analyzed"});
#                 index("cake_in_h1", doc.cake < "n" ? 1 : 0);
#             }
#         }
#         """
#         index_name = self.search_path.split('/')[-1]
#         ddoc = {'indexes': {index_name: {'index': index_function}}}
#         self.put(self.ddoc_url,
#                  headers={'Content-Type': 'application/json'},
#                  data=json.dumps(ddoc))
#
#     def _search(self, **kwargs):
#         """
#         Get the JSON response from a search 2.0 query.
#         """
#         response = self.get(self.search_url, params=kwargs)
#         return response.json()
