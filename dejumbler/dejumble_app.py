from dejumbler.dejumble import solve_puzzle_set


def main():
    freq_dict_filepath = "./resources/freq_dict.csv"

    puzzle_filepath = "./resources/puzzle_input.json"

    solutions = solve_puzzle_set(freq_dict_filepath=freq_dict_filepath, puzzles_filepath=puzzle_filepath)

    for index, solution in enumerate(solutions):
        print("For puzzle " + str(index + 1) + ", we have the following solutions: " + str(solution))


if __name__ == "__main__":
    main()