# S3Fetch

Easy to use, multi-threaded S3 download tool.

Source: [https://github.com/rxvt/s3fetch](https://github.com/rxvt/s3fetch)

Features:

- Simple to use.
- Multi-threaded, allowing you to download multiple objects concurrently (defaults to amount of cores available).
- Quickly download a subset of objects under a prefix without listing all objects.
- Filter list of objects using regular expressions.
- Uses standard Boto3 AWS SDK and standard AWS credential locations.
- Dry run mode if you just want to see what would be downloaded.

## Installation

### Requirements

- Python >= 3.7

S3Fetch is available on PyPi and be installed via one of the following methods. Prior to running it ensure you have AWS credentials configured in one of the [standard locations](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-where).

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
  --region TEXT        Bucket region. Defaults to 'us-east-1'.
  -d, --debug          Enable debug output.
  --download-dir TEXT  Download directory. Defaults to current directory.
  --regex TEXT         Filter list of available objects by regex.
  --threads INTEGER    Number of threads to use. Defaults to core count.
  --dry-run            Don't download objects.
  --delimiter TEXT     Specify the directory delimiter. Defaults to '/'
  -q, --quiet          Don't print to stdout.
  --help               Show this message and exit.
```

## Examples:

### Full example

Download using 4 threads, into `~/Downloads/tmp`, only downloading objects that end in `.dmg`.

```
$ s3fetch s3://my-test-bucket --download-dir ~/Downloads/tmp/ --threads 4  --regex '\.dmg$'
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

Download 4 objects concurrently.
```
s3fetch s3://my-test-bucket/ --threads 4
```

### Filter objects using regular expressions

Download objects ending in `.dmg`.
```
s3fetch s3://my-test-bucket/ --regex '\.dmg$'
```

