import unittest

from creeper.authority.normalizer import normalize_official


class OfficialNormalizerTests(unittest.TestCase):
    def test_normalizes_lowercase_and_whitespace(self):
        self.assertEqual(normalize_official("  WWW.Example.COM  "), "www.example.com")

    def test_rejects_non_hostname_strings(self):
        for value in (
            "https://example.com/path",
            "example.com:8080",
            "*.example.com",
            "example.com/~user",
            "example..com",
            "-bad.example",
            "bad-.example",
            "localhost",
        ):
            with self.subTest(value=value):
                self.assertIsNone(normalize_official(value))

    def test_keeps_base_and_subdomain_as_distinct_values(self):
        self.assertNotEqual(normalize_official("example.com"), normalize_official("www.example.com"))


if __name__ == "__main__":
    unittest.main()
