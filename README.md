# Lake Sharing: An Open Protocol for Secure Data Sharing

Fork of Delta-Sharing with the following goals:

- allow to share data in different table formats (iceberg, hudi, hive-style) leveraging the delta-sharing protocol and adding new APIs where needed
- lake-sharing protocol will cover not only data-sharing needs but also administration needs, such as the creation of shares, configuration of sources, etc.
- provide a production-grade server implementation of both the original delta-sharing protocol and the new lake-sharing protocol

This repository contains:

- the original delta-sharing protocol
- the lake-sharing protocol
- a reference implementation of a server that implements both protocols
- lake-sharing clients


# Reporting Issues

We use [GitHub Issues](https://github.com/agile-lab-dev/lake-sharing/issues) to track community-reported issues. You can also [contact Agile Lab](mailto:communityimpact@agilelab.it) for getting answers.

# Contributing 
We welcome contributions to Lake Sharing. See our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

# License
[Apache License 2.0](LICENSE.txt).