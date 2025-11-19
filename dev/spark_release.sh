#!/bin/bash -e -pipe

export GPG_TTY=$(tty)

# Switch to the project root directory
cd $( dirname $0 )
cd ..

# Clean up uncommitted files
git clean -fdx

# Clean existing artifacts
build/sbt clean

# Run SBT release process (includes version management, tagging, and publishing)
if build/sbt "release skip-tests"; then
  echo "=== Successfully published release artifacts ==="

  # Transfer deployment to Central Publisher Portal
  echo "Transferring deployment to Central Publisher Portal..."
  NAMESPACE="io.delta"
  OSSRH_BASE="https://ossrh-staging-api.central.sonatype.com"

  # Make POST request to upload to Portal
  # This must be done from the same IP that was used for publishing
  # You need these environment variables to be set:
  # export SONATYPE_USERNAME=your-username
  # export SONATYPE_PASSWORD=your-password
  curl -X POST \
    -H "Authorization: Bearer $(echo -n "$SONATYPE_USERNAME:$SONATYPE_PASSWORD" | base64)" \
    "$OSSRH_BASE/manual/upload/defaultRepository/$NAMESPACE" \
    && echo "✓ Successfully transferred to Central Publisher Portal" \
    || echo "✗ Failed to transfer to Central Publisher Portal"
else
  echo "✗ Release process failed, skipping transfer to Central Publisher Portal"
  exit 1
fi

echo "=== Release process completed ==="
