"""Misc utilities for S3Fetch."""
from threading import Lock

import click


def tprint(msg: str, lock: Lock, quiet: bool = False) -> None:
    """Thread safe print function.

    Args:
        msg (str): Message to print.
        lock (Lock): Lock object.
        quiet (bool, optional): Quiet mode enabled. Defaults to False.
    """
    lock.acquire(timeout=1)
    if not quiet:
        click.echo(msg)
    lock.release()
