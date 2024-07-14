# Contributing

## 1Password devcontainer setup

1. Ensure you have your SSH_AUTH_SOCK environment variable setup correctly on your host machine. See [this doc](https://developer.1password.com/docs/ssh/get-started/#step-4-configure-your-ssh-or-git-client). You'll need to restart your term & VSCode after setting this. You can check it's set correctly by running `echo $SSH_AUTH_SOCK` and `ssh-add -l` in a VSCode terminal.
1. The devcontainer `post_start.sh` script removes the global git config `gpg.ssh.program` to get 1Password integration working inside the devcontainer, if this causes problems for other configurations let me know and we can put some smarts in the script.

### References

* https://davejansen.com/linux-1password-ssh-git-signing-vscode-dev-containers/
* https://vinialbano.com/how-to-sign-git-commits-with-1password/#enabling-git-commit-signing-in-vscode-using-dev-containers
* https://developer.1password.com/docs/ssh/get-started/#step-4-configure-your-ssh-or-git-client

## Setup development environment using Hatch

1. Clone the repository
1. Install the dependencies.

    S3Fetch uses Hatch to manage dependencies, but we also provide a standard Requirements file that's automatically kept in sync - see below.

    To install your environment using Hatch and open a shell in it, run the following command:

    ```bash
    $ hatch shell
    ```

    Make sure to activate the environment and/or specify it as the default environment in your IDE.

## Setup development environment using pip

This is an alternative (more manual) way to setup your development environment using pip. The basics are covered, but not in any great detail (the expectation is you know your way around your prefered environment and tools).

1. Create a virtualenv.
1. Activate your virtualenv.
1. Install the dependencies using pip:

    ```bash
    $ python -m pip install -r requirements/requirements-dev.txt
    ```

# Install pre-commit hooks

Inside your virtual environment, run the following command:

```
$ pre-commit install
```

# Running tests via Hatch

Coming soon...
