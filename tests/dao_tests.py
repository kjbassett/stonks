import unittest

import pandas as pd
from data_access.base_dao import construct_insert_query


class TestConstructInsertQuery(unittest.TestCase):

    def test_dict_of_lists(self):
        table = "test_table"
        data = {"column1": [1, 2, 3], "column2": ["a", "b", "c"]}
        on_conflict = None
        query, params, many = construct_insert_query(table, data, on_conflict)

        expected_query = "INSERT INTO test_table (column1, column2) VALUES (?, ?);"
        expected_params = [(1, "a"), (2, "b"), (3, "c")]

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertTrue(many)

    def test_empty_dict(self):
        table = "test_table"
        data = {}
        on_conflict = None
        with self.assertRaises(ValueError):
            construct_insert_query(table, data, on_conflict)

    def test_dict_with_single_list(self):
        table = "test_table"
        data = {"column1": [1], "column2": ["a"]}
        on_conflict = None
        query, params, many = construct_insert_query(table, data, on_conflict)

        expected_query = "INSERT INTO test_table (column1, column2) VALUES (?, ?);"
        expected_params = (1, "a")

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertFalse(many)

    def test_dict(self):
        table = "test_table"
        data = {"column1": 1, "column2": "a"}
        on_conflict = None
        query, params, many = construct_insert_query(table, data, on_conflict)

        expected_query = "INSERT INTO test_table (column1, column2) VALUES (?, ?);"
        expected_params = (1, "a")

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertFalse(many)

    def test_dataframe_with_multiple_rows(self):
        table = "test_table"
        data = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})
        on_conflict = None
        query, params, many = construct_insert_query(table, data, on_conflict)

        expected_query = "INSERT INTO test_table (column1, column2) VALUES (?, ?);"
        expected_params = [(1, "a"), (2, "b"), (3, "c")]

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertTrue(many)

    def test_dataframe_with_single_row(self):
        table = "test_table"
        data = pd.DataFrame({"column1": [1], "column2": ["a"]})
        on_conflict = None
        query, params, many = construct_insert_query(table, data, on_conflict)

        expected_query = "INSERT INTO test_table (column1, column2) VALUES (?, ?);"
        expected_params = (1, "a")

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertFalse(many)

    def test_tuple(self):
        table = "test_table"
        data = (1, "a")
        on_conflict = None
        query, params, many = construct_insert_query(table, data, on_conflict)

        expected_query = "INSERT INTO test_table VALUES (?, ?);"
        expected_params = (1, "a")

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertFalse(many)

    def test_list_of_tuples(self):
        table = "test_table"
        data = [(1, "a"), (2, "b"), (3, "c")]
        on_conflict = None
        query, params, many = construct_insert_query(table, data, on_conflict)

        expected_query = "INSERT INTO test_table VALUES (?, ?);"
        expected_params = [(1, "a"), (2, "b"), (3, "c")]

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertTrue(many)

    def test_list_of_dicts(self):
        table = "test_table"
        data = [
            {"column1": 1, "column2": "a"},
            {"column1": 2, "column2": "b"},
            {"column1": 3, "column2": "c"},
        ]
        on_conflict = None
        query, params, many = construct_insert_query(table, data, on_conflict)

        expected_query = "INSERT INTO test_table (column1, column2) VALUES (?, ?);"
        expected_params = [(1, "a"), (2, "b"), (3, "c")]

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertTrue(many)

    def test_on_conflict_update(self):
        table = "test_table"
        data = {"column1": 1, "column2": "a"}
        on_conflict = "UPDATE"
        update_cols = ["column2"]
        query, params, many = construct_insert_query(
            table, data, on_conflict, update_cols
        )

        expected_query = "INSERT INTO test_table (column1, column2) VALUES (?, ?) ON CONFLICT DO UPDATE SET column2 = excluded.column2;"
        expected_params = (1, "a")

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertFalse(many)

    def test_on_conflict_ignore(self):
        table = "test_table"
        data = {"column1": 1, "column2": "a"}
        on_conflict = "IGNORE"
        query, params, many = construct_insert_query(table, data, on_conflict)

        expected_query = "INSERT INTO test_table (column1, column2) VALUES (?, ?) ON CONFLICT DO NOTHING;"
        expected_params = (1, "a")

        self.assertEqual(expected_query, query)
        self.assertEqual(expected_params, params)
        self.assertFalse(many)

    def test_on_conflict_invalid(self):
        table = "test_table"
        data = {"column1": 1, "column2": "a"}
        on_conflict = "INVALID"
        with self.assertRaises(NotImplementedError):
            construct_insert_query(table, data, on_conflict)


if __name__ == "__main__":
    unittest.main()
