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
python3 setup.py clean --all
rm -rf delta_sharing.egg-info dist
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
python3 setup.py sdist bdist_wheel
cd ..

# Generate the pre-built server package and sign files
build/sbt server/universal:packageBin
cd server/target/universal
gpg --detach-sign --armor --sign delta-sharing-server-$VERSION.zip
gpg --verify delta-sharing-server-$VERSION.zip.asc
sha256sum delta-sharing-server-$VERSION.zip > delta-sharing-server-$VERSION.zip.sha256
sha256sum -c delta-sharing-server-$VERSION.zip.sha256
sha256sum delta-sharing-server-$VERSION.zip.asc > delta-sharing-server-$VERSION.zip.asc.sha256
sha256sum -c delta-sharing-server-$VERSION.zip.asc.sha256
cd -

# Build the docker image
build/sbt server/docker:publish

git checkout main

echo "=== Generated all release artifacts ==="
