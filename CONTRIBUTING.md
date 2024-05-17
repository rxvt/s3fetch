# Contributing

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

# Running tests via Hatch

Coming soon...
