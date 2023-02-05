"""S3Fetch Nox config."""
import nox
from nox_poetry import Session, session

PYTHON_VERSIONS = ("3.7", "3.8", "3.9", "3.10", "3.11")
MINIMUM_PYTHON_VERSION = ("3.7",)
LINT_LOCATIONS = ("src", "tests", "./noxfile.py")

# Default sessions
# nox.options.sessions = ("lint", "tests", "mypy", "safety")
nox.options.sessions = ("tests", "mypy", "safety")


@session(python=PYTHON_VERSIONS)
def tests(session: Session) -> None:
    """Run tests.

    Args:
        session (Session): Nox Session object.
    """
    args = session.posargs or ("--cov",)
    session.install(".")
    session.install("pytest")
    session.install("pytest-cov")
    session.install("pytest_mock")
    session.install("coverage[toml]")
    session.install("boto3-stubs[s3]")
    session.install("moto")
    session.run("pytest", *args)


@session(python=MINIMUM_PYTHON_VERSION)
def lint(session: Session) -> None:
    """Run various linters over source.

    Args:
        session (Session): Nox Session object.
    """
    args = session.posargs or LINT_LOCATIONS
    session.install(
        "flake8",
        "flake8-black",
        "flake8-isort",
        "flake8-bugbear",
        "flake8-bandit",
        "flake8-annotations",
        "flake8-docstrings",
        "darglint",
    )
    session.run("flake8", *args)


@session(python=MINIMUM_PYTHON_VERSION)
def black(session: Session) -> None:
    """Run Black formatter over source.

    Args:
        session (Session): Nox Session object.
    """
    args = session.posargs or LINT_LOCATIONS
    session.install("black")
    session.run("black", *args)


@session(python=MINIMUM_PYTHON_VERSION)
def mypy(session: Session) -> None:
    """Run MyPy over source.

    Args:
        session (Session): Nox Session object.
    """
    args = session.posargs or LINT_LOCATIONS
    session.install("mypy")
    session.run("mypy", *args)


@session(python=MINIMUM_PYTHON_VERSION)
def coverage(session: Session) -> None:
    """Run test coverage report."""
    args = session.posargs or ("report",)
    session.install("coverage[toml]")
    session.run("coverage", *args)


@session(python=MINIMUM_PYTHON_VERSION)
def coverage_html(session: Session) -> None:
    """Run test coverage report generating HTML output."""
    session.posargs = ("html",)
    session.install("coverage[toml]")
    coverage(session)


# Stolen from https://github.com/cjolowicz/nox-poetry/blob/main/noxfile.py
@session(python=MINIMUM_PYTHON_VERSION)
def safety(session: Session) -> None:
    """Scan dependencies for insecure packages."""
    requirements = session.poetry.export_requirements()
    session.install("safety")
    session.run("safety", "check", "--full-report", f"--file={requirements}")
