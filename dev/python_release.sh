#!/usr/bin/env bash
set -euo pipefail

# Switch to the project root directory
pushd "$(dirname "$0")/.."

# Clean existing artifacts
pushd python
python3 setup.py clean --all
rm -rf delta_sharing.egg-info dist
popd

printf "Please type the python release version: "
read -r VERSION
echo "$VERSION"

# Update the Python connector version
sed -i'' "s/^__version__ = \".*\"$/__version__ = \"$VERSION\"/" ./python/delta_sharing/version.py
git add ./python/delta_sharing/version.py
# Use --allow-empty so that we can re-run this script even if the Python connector version has been updated
git commit -m "Update Python connector version to $VERSION" --allow-empty

# This creates a lightweight tag that points to the current commit.
git tag "py-v$VERSION"

# Generate Python artifacts
pushd python
python3 setup.py sdist bdist_wheel
popd

echo "=== Generated all release artifacts ==="
popd
