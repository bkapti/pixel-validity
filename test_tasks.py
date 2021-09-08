import unittest
import pandas as pd

from tasks import ingest_file, clean_url, create_df_url


class TasksTests(unittest.TestCase):
    def test_ingest_file_values(self):
        """Test that the file is read as expected."""
        self.assertEqual(
            ingest_file(file_path=f"test_file_valid.csv")["tactic_id"][0], 1111
        )
        self.assertEqual(
            ingest_file(file_path=f"test_file_valid.csv")["impression_pixel_json"][0],
            "[]",
        )

    def test_ingest_file_no_file(self):
        """Test that if an invalid file path is passed, program should exit."""
        with self.assertRaises(SystemExit):
            df = ingest_file(file_path=f"test_file_no_file.csv")

    def test_clean_url_nan(self):
        """Test that when the impression_pixel_json value is nan it's converted to an empty list."""
        self.assertEqual(clean_url("nan"), [])

    def test_clean_url_brackets(self):
        """Test that when the impression_pixel_json value is empty brackets it's converted to an empty list."""
        self.assertEqual(clean_url("[]"), [])

    def test_clean_url_empty_string(self):
        """Test that when the impression_pixel_json value is an empty string it's converted to an empty list."""
        self.assertEqual(clean_url(""), [])

    def test_clean_url_null(self):
        """Test that when the impression_pixel_json value is NULL it's converted to an empty list."""
        self.assertEqual(clean_url("NULL"), [])

    def test_clean_url_backslashes_with_single_bracket(self):
        """Test that when the impression_pixel_json value has double back slashes it's replaced with empty strings."""
        self.assertEqual(clean_url("[\\next line"), ["next line"])

    def test_clean_url_double_quotes_with_single_bracket(self):
        """
        Test that when the impression_pixel_json value has double quotes, they are removed.
        Test that when the last character of the impression_pixel_json is "]" it's removed.
        """
        self.assertEqual(clean_url('"double quotes"]'), ["double quotes"])

    def test_clean_url_double_brackets(self):
        """Test that when the first and last character of the impression_pixel_json is "]" they are removed."""
        self.assertEqual(clean_url("[double brackets]"), ["double brackets"])

    def test_create_df_url(self):
        """Test that the url data frame is created as expected."""
        data_1 = {
            "tactic_id": [123],
            "impression_pixel_json": [
                '["https:\/\/triplelift.com","https:\/\/reebee.com"]'
            ],
        }
        df_1 = pd.DataFrame(data_1)
        data_2 = {
            "tactic_id": [123, 123],
            "url": ["https://triplelift.com", "https://reebee.com"],
        }
        df_2 = pd.DataFrame(data_2)
        self.assertEqual(create_df_url(df_1)["url"][0], df_2["url"][0])
        self.assertEqual(create_df_url(df_1)["url"][1], df_2["url"][1])


if __name__ == "__main__":
    unittest.main()
