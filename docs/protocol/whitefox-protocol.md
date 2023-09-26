# API

To qualify all the APIs as:
- **supported**
- **minor change**
- **breaking change**


## Concepts:

Towards recipients, the logical organization of tables is the following:

![external_logical_org](imgs/external_logical_org.drawio.png)

- Share: A share is a logical grouping to share with recipients. A share can be shared with one or multiple recipients. A recipient can access all resources in a share. A share may contain multiple schemas.
- Schema: A schema is a logical grouping of tables. A schema may contain multiple tables.
- Table: A table is a Delta Lake table or a view on top of a Delta Lake table.
- Recipient: A principal that has a bearer token to access shared tables.
- Sharing Server: A server that implements this protocol.

Internally organization is a bit more complex: 

![internal logical org](imgs/internal_logical_org.drawio.png)

- Provider: a composed entity that allows to access a table existing in an external system. A provider is composed by a storage system and optionally a metastore (or catalog?)
- Storage: an object storage that contains the table data
- Metastore: a system that provides metadata about tables, it is mandatory for Iceberg tables and can be useful for discovery in general

## Users

Initially we define two kind of users:

1. admins
2. guests

Admins can:

- create/update/delete storages
- create/update/delete metastores
- create/update/delete providers
- create/update/delete shares
- create/update/delete schemas
- create/update/delete tables
- add guests as recipients of shares

## STORAGES

## METASTORES

## PROVIDERS


## SHARES
- (from delta sharing [API page](https://github.com/agile-lab-dev/whitefox/blob/main/PROTOCOL.md) )

## SCHEMAs/TABLEs
- (from delta sharing [API page](https://github.com/agile-lab-dev/whitefox/blob/main/PROTOCOL.md))

## ACCESS CONTROL
- (from Databricks [documentation page](https://docs.databricks.com/api/workspace/shares/get) )

## DATA ACCESS
- (from delta sharing [API page](https://github.com/agile-lab-dev/whitefox/blob/main/PROTOCOL.md))