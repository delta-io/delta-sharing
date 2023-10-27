# Whitefox: An Open Protocol for Secure Data Sharing

Fork of Delta-Sharing with the following goals:

- allow to share data in different table formats (iceberg, hudi, hive-style) leveraging the delta-sharing protocol and adding new APIs where needed
- whitefox protocol will cover not only data-sharing needs but also administration needs, such as the creation of 
  shares, configuration of sources, etc.
- provide a production-grade server implementation of both the original delta-sharing protocol and the new whitefox 
  protocol

This repository contains:

- the original delta-sharing protocol
- the whitefox protocol
- a reference implementation of a server that implements both protocols
- whitefox clients

# Protocols

Whitefox protocol is rendered [here](https://agile-lab-dev.github.io/whitefox/openapi_whitefox.html)

The original delta-sharing protocol is rendered [here](https://agile-lab-dev.github.io/whitefox/openapi_delta-sharing.html)


# Reporting Issues

We use [GitHub Issues](https://github.com/agile-lab-dev/whitefox/issues) to track community-reported issues. You can also [contact Agile Lab](mailto:communityimpact@agilelab.it) for getting answers.

# Contributing 
We welcome contributions to Whitefox. See our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

# License
[Apache License 2.0](LICENSE.txt).