# Check if string x is a subsequence of string y (all characters in string x exist in string y)
def is_subsequence(x, y):
    for char in x:
        if char not in y:
            return False
    return True

# Sort string in alphabetical order
def alphabetize_string(string):
    return ''.join(sorted(string))