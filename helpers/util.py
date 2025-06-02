"""
This file contains useful functions leveraged across the codebase.
"""


def log_based_on_poll_date(logger, message, is_bad_poll_date: bool):
    # Log as normal message if it's a known issue, otherwise
    # send an error message
    if is_bad_poll_date:
        logger.info(message)
    else:
        logger.error(message)
