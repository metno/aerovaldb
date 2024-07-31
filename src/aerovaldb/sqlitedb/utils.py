import re


def extract_substitutions(template: str):
    """
    For a python template string, extracts the names between curly brackets:

    For example 'blah blah {test} blah {test2}' returns [test, test2]
    """
    return re.findall(r"\{(.*?)\}", template)
