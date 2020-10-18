import nox
import nox_poetry.patch
from nox.sessions import Session


@nox.session(python=["3.9"])
def tests(session: Session) -> None:
    session.install(".")
    session.install("pytest")
    session.install("pytest-mock")
    session.run("pytest")


@nox.session(python="3.9")
def coverage(session: Session) -> None:
    session.install(".")
    session.install("pytest")
    session.install("pytest-cov")
    session.run("pytest", "--cov=src/s3fetch", "--cov-report=term-missing", "tests/")