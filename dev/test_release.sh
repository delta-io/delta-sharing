#!/bin/bash -e -pipe

export GPG_TTY=$(tty)

# Switch to the project root directory
cd $( dirname $0 )
cd ..

# Clean up uncommitted files
git clean -fdx

# Clean existing artifacts
build/sbt clean

printf "Please type the test version (will be suffixed with -TEST-SNAPSHOT): "
read BASE_VERSION
TEST_VERSION="${BASE_VERSION}-TEST-SNAPSHOT"
echo "Publishing test version: $TEST_VERSION"

# Set test version and publish to snapshots (without signing for testing)
build/sbt "set version := \"$TEST_VERSION\"" -Dtest.publish=true publish

echo "=== Published test artifacts to snapshots repository ==="
echo "Test version: $TEST_VERSION"
echo "Repository: snapshots (safe for testing)" 