from threading import Lock

import click


def tprint(msg: str, lock: Lock, quiet: bool = False) -> None:
    lock.acquire(timeout=1)
    if not quiet:
        click.echo(msg)
    lock.release()
