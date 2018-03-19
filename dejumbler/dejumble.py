import pyspark.sql.functions as sql_functions
import json
from JumblePuzzles import JumblePuzzles
from Error import InputError
from dejumbler.spark import get_spark
from util_functions import is_subsequence
from util_functions import alphabetize_string


# This function reads in the frequency dictionary .csv (of format NUMBER,FREQUENCY)
# and returns a DataFrame with a column that shows the word sorted by alphabetical order.
def process_frequency_dictionary(spark, file_path):

    # 9887 is the least frequent ranked frequency
    UNRANKED_FREQUENCY = 9888

    # Read in file as csv, using header to form schema
    frequency_dict_df = spark.read.csv(file_path, header=True)

    # Use this UDF to add an alphabetically sorted version of each word to each row,
    # with the intention to use this as a key to a hashmap-like structure to unscramble
    # words.
    sort_string = sql_functions.udf(lambda s: alphabetize_string(s))
    # Use this UDF to assign a length to each word in the frequency dictionary.
    word_length = sql_functions.udf(lambda s: len(s))

    # Add UDF columns, and change all unranked frequencies (default 0) to the maximum frequency + 1 so it does not
    # interfere with our sorting based on frequency.
    processed_freq_dict_df = frequency_dict_df\
        .withColumn("sorted_word", sort_string("word"))\
        .withColumn("word_length", word_length("word"))\
        .withColumn("frequency", sql_functions.when(frequency_dict_df["frequency"] == 0, UNRANKED_FREQUENCY)
                    .otherwise(frequency_dict_df["frequency"]))

    return processed_freq_dict_df


# This acts as a wrapper object for the JSON input object that contains the list of puzzles.
def process_puzzle_list(file_path):

    with open(file_path) as f:
        puzzle_list = json.load(f, object_hook=JumblePuzzles)
    return puzzle_list


# Solve each jumble by alphabetizing the jumble string and searching for a match in the frequency dictionary
# TODO: Enhance to provide multiple possible solutions to jumble
def process_jumble(jumble, freq_dict_df):

    sorted_jumble = alphabetize_string(jumble.letters)

    # Match the alphabetized string to a corresponding value in the dictionary (using ranked words first)
    matches_df = freq_dict_df\
        .filter(freq_dict_df['sorted_word'] == sorted_jumble)\
        .select("word", "frequency")\
        .sort("frequency")

    # Take the first item of the frequency-sorted list (so we get the most common word)
    if matches_df.head():

        match = matches_df.head().word

        circled_indices = jumble.circled_indices

        circled_letters = "".join(match[i] for i in circled_indices)

    else:
        raise InputError(expression=jumble.letters, message="No potential anagrams found in frequency"
                                                            " dictionary for provided input " + jumble.letters)
    return match, circled_letters


# Solve each final jumble by finding the most likely word given the circled letters from the previous jumbles and
# the word count/length of the solution.
# TODO: Account for cases where words should be determined in order other than left-to-right
def process_final_jumble(final_jumble, letters, freq_dict_df):
    word_count = final_jumble.word_count
    word_lengths = final_jumble.word_lengths

    if word_count != len(word_lengths):
        raise InputError(expression=final_jumble, message="Error in input data, final jumble's word count "
                                                          "and word_lengths field do not align")

    filtered_matches_df = freq_dict_df \
        .select("word", "frequency", "sorted_word", "word_length") \
        .sort("frequency")

    letter_pool = alphabetize_string(letters)
    final_jumble_answer = []

    # Use circled letters to identify most common words given a target word length
    for word_length in word_lengths:

        # Filter for words that would fit in the current letter pool
        try:
            current_matches_df = filtered_matches_df\
                .filter(freq_dict_df['word_length'] == word_length) \
                .rdd.filter(lambda row: is_subsequence(row['word'], letter_pool)).toDF()
        except ValueError:
            # TODO: Handle cases where this DF comes up empty because we are parsing solutions left-to-right only
            pass

        # If we a match, add the word to the jumble answer, and delete its constituent characters from the pool
        if current_matches_df.head():
            word = current_matches_df.head().word
            final_jumble_answer.append(word)
            for char in word:
                letter_pool = letter_pool.replace(char, "", 1)

    return final_jumble_answer


# This method solves each puzzle by solving each jumble, accumulating the circled letters from each jumble solution,
# and using them to solve the final jumble riddle.
def solve_puzzle(puzzle, freq_dict_df):

    solutions = []
    final_jumble_letter_pool = ""

    for jumble in puzzle.jumbles:
        solution, circled_letters = process_jumble(jumble, freq_dict_df)

        solutions.append(solution)
        final_jumble_letter_pool = final_jumble_letter_pool + circled_letters

    final_answer = process_final_jumble(final_jumble=puzzle.final_jumble, letters=final_jumble_letter_pool, freq_dict_df=freq_dict_df)

    solutions.append(" ".join(final_answer))

    return solutions


# This method reads in the input JSON list of puzzles and the frequency dictionary .csv
# and provides the puzzles' solutions
def solve_puzzle_set(freq_dict_filepath, puzzles_filepath):

    spark = get_spark()
    freq_dict_df = process_frequency_dictionary(spark=spark, file_path=freq_dict_filepath)

    puzzles_input = process_puzzle_list(puzzles_filepath)

    puzzle_solutions = []
    for puzzle in puzzles_input.puzzles:
        puzzle_solutions.append(solve_puzzle(puzzle, freq_dict_df))

    return puzzle_solutions
