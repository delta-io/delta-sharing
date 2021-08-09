#!/bin/bash -e -pipe

# Switch to the project root directory
cd $( dirname $0 )
cd ..

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
git commit -m "Update Python connector version to $VERSION"

build/sbt "release skip-tests"

# Switch to the release commit
git checkout v$VERSION

# Generate Python artifacts
cd python/
python3 setup.py sdist bdist_wheel
cd ..

# Generate the pre-built server package and sign files
build/sbt server/universal:packageBin
sha256sum server/target/universal/delta-sharing-server-$VERSION.zip > server/target/universal/delta-sharing-server-$VERSION.zip.sha256
gpg --detach-sign --armor --sign server/target/universal/delta-sharing-server-$VERSION.zip
gpg --detach-sign --armor --sign server/target/universal/delta-sharing-server-$VERSION.zip.sha256
gpg --verify server/target/universal/delta-sharing-server-$VERSION.zip.asc
gpg --verify server/target/universal/delta-sharing-server-$VERSION.zip.sha256.asc

# Build the docker image
build/sbt server/docker:publish

git checkout main

echo "=== Generated all release artifacts ==="
