import re


def clean_space(string):
    """
    Replace underscores by spaces, replace non-breaking space by normal space,
    remove exotic whitespace, normalize duplicate spaces, trim whitespace
    (any char <= U+0020) from start and end.
    Method adapted from org.dbpedia.extraction.util.WikiUtil class (extraction-framework package).
    :param string:
    :return:
    """
    replacement_char = ' '
    replacement_map = {
        ' ': replacement_char,
        '_': replacement_char,
        '\u00A0': replacement_char,
        '\u200E': replacement_char,
        '\u200F': replacement_char,
        '\u2028': replacement_char,
        '\u202A': replacement_char,
        '\u202B': replacement_char,
        '\u202C': replacement_char,
        '\u3000': replacement_char
    }

    res = ''
    last = replacement_char

    for char in string:
        if char in replacement_map:
            replaced = replacement_map[char]
            if last != replaced:
                res = f'{res}{replaced}'
            last = replaced
        else:
            res = f'{res}{char}'
            last = char

    return res.strip()


def split_camelcase(string):
    """
    Split a camelCase string (consecutive uppercase chars are considered as keywords).
    Examples:
    -- CamelCase123 -> Camel Case123
    -- CamelCaseXYZ -> Camel Case XYZ
    -- XYZ -> XYZ
    :param string:
    :return:
    """
    return " ".join(re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', string)).split())
