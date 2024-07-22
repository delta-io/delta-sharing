#!/bin/bash -e -pipe

export GPG_TTY=$(tty)

# Switch to the project root directory
cd $( dirname $0 )
cd ..

# Clean up uncommitted files
git clean -fdx

# Clean existing artifacts
build/sbt clean

printf "Please type the release version: "
read VERSION
echo $VERSION

build/sbt "release skip-tests"

# Switch to the release commit
git checkout v$VERSION

echo "=== Generated all release artifacts ==="