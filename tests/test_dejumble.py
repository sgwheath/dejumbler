
import unittest
from dejumbler.dejumble import solve_puzzle_set


class TestDejumble(unittest.TestCase):

    def test_puzzle_solver(self):
        freq_dict_filepath = "./resources/freq_dict_sample.csv"

        puzzle_filepath = "./resources/puzzle_input_sample.json"
        solutions = solve_puzzle_set(freq_dict_filepath=freq_dict_filepath, puzzles_filepath=puzzle_filepath)

        self.assertEqual(solutions, ['gland', 'major', 'becalm', 'lawyer', 'jab well done'])

    # TODO: unit tests for components of solve_puzzle_set (process_jumble, process_final_jumble, etc.)


if __name__ == '__main__':
    unittest.main()
