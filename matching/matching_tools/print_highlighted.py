import re

COLORS = {'red': 91,
          'green': 92,
          'yellow': 93,
          'light_purple': 94,
          'purple': 95,
          'cyan': 96,
          'light_gray': 97}


def print_highlighted(message, pattern, color='green'):
    """
    Display a string with highlighted substrings
    :param message: original string
    :param pattern: pattern to highlight of a list of patterns
    :param color: highlighting color
    :return: None
    """

    if color not in COLORS.keys():
        raise ValueError("currently only the following colors are supported: {}".format(list(COLORS.keys())))

    if isinstance(pattern, list):
        message_ = message
        for ptrn in pattern:
            replace = '\033[{}m'.format(COLORS[color]) + ptrn + '\033[00m'
            message_ = re.sub(ptrn, replace, message_)
        print(message_)
    else:
        replace = '\033[{}m'.format(COLORS[color]) + pattern + '\033[00m'
        print(re.sub(pattern, replace, message))


def format_highlighted(message, pattern, color='green'):
    """
    Display a string with highlighted substrings
    :param message: original string
    :param pattern: pattern to highlight of a list of patterns
    :param color: highlighting color
    :return: None
    """

    if color not in COLORS.keys():
        raise ValueError("currently only the following colors are supported: {}".format(list(COLORS.keys())))

    if isinstance(pattern, list):
        message_ = message
        for ptrn in pattern:
            replace = '\033[{}m'.format(COLORS[color]) + ptrn + '\033[00m'
            message_ = re.sub(ptrn, replace, message_)
        return message_
    else:
        replace = '\033[{}m'.format(COLORS[color]) + pattern + '\033[00m'
        return re.sub(pattern, replace, message)


# if __name__ == "__main__":
#      print_highlighted("ab cdde12", ['12', 'cd'], color='purple')