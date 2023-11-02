---
sidebar_position: 1
---

# Introduction

Whitefox is a fork of Delta-Sharing with the following goals:

- allow to share data in different table formats (iceberg, hudi, hive-style) leveraging the delta-sharing protocol and adding new APIs where needed
- whitefox protocol will cover not only data-sharing needs but also administration needs, such as the creation of 
  shares, configuration of sources, etc.
- provide a production-grade server implementation of both the original delta-sharing protocol and the new whitefox 
  protocol

The [protocols](./protocols) page contains the original delta-sharing protocol and whitefox protocol.

[Development Guidelines](./development_guidelines.md) contains relevant info to whoever wants to contribute to Whitefox and get started.

[Architecture](./architecture) contains architectural design decisions.
