# Memo 001

## Strategic decisions:

- Client libs coupling: we don’t want to embed into our lib the ones related to the support table and file formats.
- Our clients will have a soft dependency, the users will need to couple their own Iceberg/Hudi/Delta libs and load in their runtime
- The versions used for writing is included in the metadata, we assume the clients will always be able to read with libs version x everything written with versions <= x.
- The server will load its own dependencies (ideally latest iceberg/delta/hudi standalone libraries)
- Follow a cloud-native and decoupled approach for infrastructural dependencies (e.g. the underlying DB to host metadata shouldn't be a hard dependency, users should be able to plug the DB of their choice as long as it provides the APIs we need)