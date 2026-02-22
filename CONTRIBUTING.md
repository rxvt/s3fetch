# Contributing

## 1Password devcontainer setup

1. Ensure you have your SSH_AUTH_SOCK environment variable setup correctly on your host machine. See [this doc](https://developer.1password.com/docs/ssh/get-started/#step-4-configure-your-ssh-or-git-client). You'll need to restart your term & VSCode after setting this. You can check it's set correctly by running `echo $SSH_AUTH_SOCK` and `ssh-add -l` in a VSCode terminal.
1. The devcontainer `post_start.sh` script removes the global git config `gpg.ssh.program` to get 1Password integration working inside the devcontainer, if this causes problems for other configurations let me know and we can put some smarts in the script.

### References

* https://davejansen.com/linux-1password-ssh-git-signing-vscode-dev-containers/
* https://vinialbano.com/how-to-sign-git-commits-with-1password/#enabling-git-commit-signing-in-vscode-using-dev-containers
* https://developer.1password.com/docs/ssh/get-started/#step-4-configure-your-ssh-or-git-client

## Setup development environment using Hatch

1. Clone the repository.
1. Install [hatch](https://hatch.pypa.io/) using [uv](https://docs.astral.sh/uv/):

    ```bash
    uv tool install hatch --with hatch-pip-compile
    ```

1. Create the development environment:

    ```bash
    hatch env create
    ```

    This installs all dev and test dependencies and sets up pre-commit hooks automatically.

1. Run s3fetch from source to verify the setup:

    ```bash
    hatch run s3fetch --help
    ```

## Running tests

Run unit tests:

```bash
just test-unit
```

Run integration tests:

```bash
just test-integration
```

Run all tests across all supported Python versions:

```bash
just test -a
```

Run a single test:

```bash
just test tests/unit/test_cli.py::TestValidateThreadCount
```

## Linting and formatting

```bash
just lint
just format
```

Or run all quality checks at once:

```bash
just check
```

## Pre-commit hooks

Pre-commit hooks are installed automatically when you run `hatch env create`. To install them manually:

```bash
pre-commit install
```
