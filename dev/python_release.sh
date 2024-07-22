#!/bin/bash -e -pipe

# Switch to the project root directory
cd $( dirname $0 )
cd ..

# Clean up uncommitted files
#git clean -fdx

python3 -m venv .venv
source .venv/bin/activate
pip install maturin
maturin develop
pip install pyarrow
pip install pandas

cd python
python3 setup.py clean --all
rm -rf delta_sharing.egg-info dist
cd ..

printf "Please type the python release version: "
read VERSION
echo $VERSION

# Update the Python connector version
sed -i '' "s/__version__ = \".*\"/__version__ = \"$VERSION\"/g"  python/delta_sharing/version.py
git add python/delta_sharing/version.py
# Use --allow-empty so that we can re-run this script even if the Python connector version has been updated
git commit -m "Update Python connector version to $VERSION" --allow-empty

# Switch to the release commit
git checkout v$VERSION

# Generate Python artifacts
cd python/
python3 setup.py sdist bdist_wheel
cd ..

echo "=== Generated all release artifacts ==="
