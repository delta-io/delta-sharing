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

echo "Publishing snapshot version: $CURRENT_VERSION"

# Publish snapshots using version from version.sbt
if build/sbt publishSigned; then
  echo "=== Published test artifacts to snapshots repository ==="
  echo "Published version: $CURRENT_VERSION"

  # Transfer deployment to Central Publisher Portal (required for Maven-API-like plugins)
  echo "Transferring deployment to Central Publisher Portal..."
  NAMESPACE="io.delta"
  OSSRH_BASE="https://ossrh-staging-api.central.sonatype.com"

  # Make POST request to upload to Portal
  # This must be done from the same IP that was used for publishing
  curl -X POST \
    -H "Authorization: Bearer $(echo -n "$SONATYPE_USERNAME:$SONATYPE_PASSWORD" | base64)" \
    "$OSSRH_BASE/manual/upload/defaultRepository/$NAMESPACE" \
    && echo "✓ Successfully transferred to Central Publisher Portal" \
    || echo "✗ Failed to transfer to Central Publisher Portal"
else
  echo "✗ Publishing failed, skipping transfer to Central Publisher Portal"
  exit 1
fi

echo "Repository: snapshots (safe for testing)" 