import unittest
import problems


class TestProblems(unittest.TestCase):

    def test_find_circular_list_start(self):
        self.assertEqual(1,
            problems.find_start_of_circular_list([9, 1, 2, 3, 5]))
        self.assertEqual(0,
            problems.find_start_of_circular_list([1, 2, 3, 5]))
        self.assertEqual(3,
            problems.find_start_of_circular_list([2, 3, 5, 1]))
        self.assertEqual(3,
            problems.find_start_of_circular_list([2, 3, 5, 1, 1]))
        self.assertEqual(3,
            problems.find_start_of_circular_list([1, 3, 5, 1, 1]))


if __name__ == '__main__':
    unittest.main()