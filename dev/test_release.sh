#!/bin/bash -e -pipe

export GPG_TTY=$(tty)

# Switch to the project root directory
cd $( dirname $0 )
cd ..

# Clean up uncommitted files
git clean -fdx

# Clean existing artifacts
build/sbt clean

# Read current version from version.sbt
CURRENT_VERSION=$(grep "version in ThisBuild" version.sbt | sed 's/.*:= "\(.*\)"/\1/')
echo "Using version from version.sbt: $CURRENT_VERSION"

# Ensure it's a snapshot version
if [[ ! "$CURRENT_VERSION" =~ -SNAPSHOT$ ]]; then
  echo "Warning: version.sbt does not contain a SNAPSHOT version!"
  echo "Current version: $CURRENT_VERSION"
  exit 1
fi

echo "Publishing snapshot version: $CURRENT_VERSION"

# Publish snapshots using version from version.sbt
build/sbt publish

echo "=== Published test artifacts to snapshots repository ==="
echo "Published version: $CURRENT_VERSION"
echo "Repository: snapshots (safe for testing)" 