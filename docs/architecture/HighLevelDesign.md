# High Level Design

## Strategic decisions:

- Libs coupling: we don’t want to embed into our lib the ones related to the support table and file formats.
- Our clients will have a soft dependency, the users will need to couple their own Iceberg/Hudi/Delta libs and load in their runtime
- The versions used for writing is included in the metadata, we assume the clients will always be able to read with libs version x everything written with versions < = x.
- The server will probably have to load its own dependencies
- Implementation language: Java to include a broader scope of potential OSS contributors
- Follow a cloud-native and decoupled approach for infrastructural dependencies (e.g. the underlying DB to host metadata shouldn't be a hard dependency, users should be able to plug the DB of their choice as long as it provides the APIs we need)
- TBD Understand forking logic (how much can we reuse VS how much we need to change VS how much we need to implement from scratch?)