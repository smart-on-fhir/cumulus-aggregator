"""decorators shared across lambda functions"""

import functools
import logging

from src.handlers.shared.functions import http_response


def generic_error_handler(msg="Internal server error"):
    """returns 500 error when a lambda hits an error not explictly handled"""

    def error_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
            except Exception as e:  # pylint: disable=broad-except
                logging.error(str(e))
                logging.error(args[0])
                logging.error(args[1])
                res = http_response(500, msg)
            return res

        return wrapper

    return error_decorator
