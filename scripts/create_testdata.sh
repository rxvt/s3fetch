#!/usr/bin/env bash

set -e
set -x
set -o pipefail

BUCKET_NAME="s3fetch-ci-test-bucket"

echo "small test file" > smallfile.txt

for i in {1..10}; do
    aws --profile s3fetch-testing s3 cp ./smallfile.txt s3://${BUCKET_NAME}/smallfile-${i}.txt
done

for i in {1..10}; do
    for j in {1..10}; do
        aws --profile s3fetch-testing s3 cp ./smallfile.txt s3://${BUCKET_NAME}/smallfile-${i}/smallfile-${j}.txt
    done
done
rm smallfile.txt

dd if=/dev/random of=largefile.txt count=50 bs=1M
for i in {1..10}; do
    aws --profile s3fetch-testing s3 cp ./largefile.txt s3://${BUCKET_NAME}/largefiles/largefile-${i}.txt
done
rm largefile.txt
