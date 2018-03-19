class Error(Exception):
    # Base class for exceptions in this module.
    pass


class InputError(Error):
    # Exception raised for errors in the input files

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message