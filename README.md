# S3Fetch

Simple & fast multi-threaded S3 download tool.

Source: [https://github.com/rxvt/s3fetch](https://github.com/rxvt/s3fetch)

![Build and Publish](https://github.com/rxvt/s3fetch/workflows/Build%20and%20Publish/badge.svg?branch=main)
![Test](https://github.com/rxvt/s3fetch/workflows/Test/badge.svg?branch=development)
[![PyPI version](https://badge.fury.io/py/s3fetch.svg)](https://badge.fury.io/py/s3fetch)

## Features

- Fast.
- Simple to use.
- Multi-threaded, allowing you to download multiple objects concurrently.
- Quickly download a subset of objects under a prefix without listing all objects first.
- Object listing occurs in a seperate thread and downloads start as soon as the first object key is returned while the object listing completes in the background.
- Filter list of objects using regular expressions.
- Uses standard Boto3 AWS SDK and standard AWS credential locations.
- List only mode if you just want to see what would be downloaded.

## Why use S3Fetch?

Tools such as the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html) and [s4cmd](https://pypi.org/project/s4cmd/) are great and offer a lot of features, but S3Fetch out performs them when downloading a subset of objects from a large S3 bucket.

Benchmarking shows (see below) that S3Fetch can finish downloading 428 objects from a bucket containing 12,204,097 objects in 8 seconds while other tools have not started downloading a single object after 60 minutes.

## Benchmarks

Downloading 428 objects under the `fake-prod-data/2020-10-17` prefix from a bucket containing a total of 12,204,097 objects.

#### With 100 threads

```text
s3fetch s3://fake-test-bucket/fake-prod-data/2020-10-17  --threads 100

8.259 seconds
```

```text
s4cmd get s3://fake-test-bucket/fake-prod-data/2020-10-17* --num-threads 100

Timed out while listing objects after 60min.
```

#### With 8 threads
```text
s3fetch s3://fake-test-bucket/fake-prod-data/2020-10-17  --threads 8

29.140 seconds
```

```text
time s4cmd get s3://fake-test-bucket/fake-prod-data/2020-10-17* --num-threads 8

Timed out while listing objects after 60min.
```

## Installation

### Requirements

- Python >= 3.7
- AWS credentials in one of the [standard locations](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-where)

S3Fetch is available on PyPi and can be installed via one of the following methods.

### pipx (recommended)

Ensure you have [pipx](https://pypi.org/project/pipx/) installed, then:

`pipx install s3fetch`


### pip

`pip3 install s3fetch`


## Usage:

```
Usage: s3fetch [OPTIONS] S3_URI

  Easily download objects from an S3 bucket.

  Example: s3fetch s3://my-test-bucket/birthday-photos/2020-01-01

  The above will download all S3 objects located under the `birthday-
  photos/2020-01-01` prefix.

  You can download all objects in a bucket by using `s3fetch s3://my-test-
  bucket/`

Options:
  --region TEXT           Bucket region. Defaults to 'us-east-1'.
  -d, --debug             Enable debug output.
  --download-dir TEXT     Download directory. Defaults to current directory.
  -r, --regex TEXT        Filter list of available objects by regex.
  -t, --threads INTEGER   Number of threads to use. Defaults to core count.
  --dry-run, --list-only  List objects only, do not download.
  --delimiter TEXT        Specify the "directory" delimiter. Defaults to '/'.
  -q, --quiet             Don't print to stdout.
  --version               Print out version information.
  --help                  Show this message and exit.
```

## Examples:

### Full example

Download using 100 threads into `~/Downloads/tmp`, only downloading objects that end in `.dmg`.

```
$ s3fetch s3://my-test-bucket --download-dir ~/Downloads/tmp/ --threads 100  --regex '\.dmg$'
test-1.dmg...done
test-2.dmg...done
test-3.dmg...done
test-4.dmg...done
test-5.dmg...done
```

### Download all objects from a bucket

```
s3fetch s3://my-test-bucket/
```

### Download objects with a specific prefix 

Download all objects that strt with `birthday-photos/2020-01-01`.
```
s3fetch s3://my-test-bucket/birthday-photos/2020-01-01
```

### Download objects to a specific directory

Download objects to the `~/Downloads` directory.
```
s3fetch s3://my-test-bucket/ --download-dir ~/Downloads
```

### Download multiple objects concurrently

Download 100 objects concurrently.
```
s3fetch s3://my-test-bucket/ --threads 100
```

### Filter objects using regular expressions

Download objects ending in `.dmg`.
```
s3fetch s3://my-test-bucket/ --regex '\.dmg$'
```

