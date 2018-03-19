import unittest
from util_functions import is_subsequence
from util_functions import alphabetize_string


class TestUtilFunctions(unittest.TestCase):

    def test_is_subsequence(self):
        subsequence = "ape"
        not_a_subsequence = "zap"

        sequence = "apple"

        self.assertTrue(is_subsequence(subsequence, sequence))
        self.assertFalse(is_subsequence(not_a_subsequence, sequence))

    def test_alphabetize_string(self):
        unsorted_string = "antidisestablishmentarianism"
        sorted_string = "aaaabdeehiiiiilmmnnnrssssttt"

        self.assertEqual(alphabetize_string(unsorted_string), sorted_string)


if __name__ == '__main__':
    unittest.main()
