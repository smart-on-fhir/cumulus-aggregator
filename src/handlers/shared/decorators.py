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
            except Exception as e:
                trace = []
                tb = e.__traceback__
                while tb is not None:
                    trace.append(
                        {
                            "filename": tb.tb_frame.f_code.co_filename,
                            "name": tb.tb_frame.f_code.co_name,
                            "lineno": tb.tb_lineno,
                        }
                    )
                    tb = tb.tb_next
                logging.error(
                    "Error: %s, type: %s, event: %s, "
                    "context: %s, file: %s, traceback: %s",
                    msg,
                    str(e),
                    args[0],
                    args[1],
                    __file__,
                    trace,
                )
                res = http_response(500, msg)
            return res

        return wrapper

    return error_decorator
