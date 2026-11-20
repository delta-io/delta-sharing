#!/bin/bash -e -pipe

export GPG_TTY=$(tty)

# Switch to the project root directory
cd $( dirname $0 )
cd ..

# Clean up uncommitted files
git clean -fdx

# Clean existing artifacts
build/sbt clean
cd python
rm -rf build dist delta_sharing.egg-info
cd ..

printf "Please type the release version: "
read VERSION
echo $VERSION

# Update the Python connector version
sed -i '' "s/__version__ = \".*\"/__version__ = \"$VERSION\"/g"  python/delta_sharing/version.py
git add python/delta_sharing/version.py
# Use --allow-empty so that we can re-run this script even if the Python connector version has been updated
git commit -m "Update Python connector version to $VERSION" --allow-empty

build/sbt "release skip-tests"

# Switch to the release commit
git checkout v$VERSION

# Generate Python artifacts
cd python/
uv build
cd ..

# Delta Sharing reference server is deprecated and no longer released; use OpenSharing Server instead.

git checkout main

echo "=== Generated all release artifacts ==="
