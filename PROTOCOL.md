<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
# Delta Sharing Protocol

- [Overview](#overview)
- [Delta Sharing Specification](#delta-sharing-specification)
  - [Concepts](#concepts)
  - [REST APIs](#rest-apis)
    - [List Shares](#list-shares)
    - [Get Share](#get-share)
    - [List Schemas in a Share](#list-schemas-in-a-share)
    - [List Tables in a Schema](#list-tables-in-a-schema)
    - [List all Tables in a Share](#list-all-tables-in-a-share)
    - [Query Table Version](#query-table-version)
    - [Query Table Metadata](#query-table-metadata)
    - [Read Data from a Table](#read-data-from-a-table)
      - [Request Body](#request-body)
  - [Table Metadata Format](#table-metadata-format)
    - [JSON Wrapper Object In Each Line](#json-wrapper-object-in-each-line)
    - [Protocol](#protocol)
    - [Metadata](#metadata)
    - [File](#file)
    - [Format](#format)
    - [Schema Object](#schema-object)
      - [Struct Type](#struct-type)
      - [Struct Field](#struct-field)
      - [Primitive Types](#primitive-types)
      - [Array Type](#array-type)
      - [Map Type](#map-type)
      - [Example](#example)
    - [Partition Value Serialization](#partition-value-serialization)
    - [Per-file Statistics](#per-file-statistics)
  - [SQL Expressions for Filtering](#sql-expressions-for-filtering)
- [Profile File Format](#profile-file-format)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Overview

[Delta Sharing](https://delta.io/sharing) is an open protocol for secure real-time exchange of large datasets, which enables secure data sharing across products for the first time. It is a simple REST protocol that securely shares access to part of a cloud dataset. It leverages modern cloud storage systems, such as S3, ADLS, or GCS, to reliably transfer large datasets.

With Delta Sharing, the user accessing shared data can directly connect to it through Pandas, Tableau, or dozens of other systems that implement the open protocol, without having to deploy a specific platform first. This reduces their access time from months to minutes, and makes life dramatically simpler for data providers who want to reach as many users as possible.

This document is a specification for the Delta Sharing Protocol, which defines the REST APIs and the formats of messages used by any clients and servers to exchange data.

# Delta Sharing Specification

## Concepts

- Share: A share is a logical grouping to share with recipients. A share can be shared with one or multiple recipients. A recipient can access all resources in a share. A share may contain multiple schemas.
- Schema: A schema is a logical grouping of tables. A schema may contain multiple tables.
- Table: A table is a [Delta Lake](https://delta.io/) table or a view on top of a Delta Lake table.
- Recipient: A principal that has a bearer token to access shared tables.
- Sharing Server: A server that implements this protocol.

## REST APIs

Here are the list of APIs used by Delta Sharing Protocol. All of the REST APIs use [bearer tokens for authorization](https://tools.ietf.org/html/rfc6750). The `{prefix}` of each API is configurable, and servers hosted by different providers may pick up different prefixes.

### List Shares

This is the API to list shares accessible to a recipient.

HTTP Request | Value
-|-
Method | `GET`
Header | `Authorization: Bearer {token}`
URL | `{prefix}/shares`
Query Parameters | **maxResults** (type: Int32, optional): The maximum number of results per page that should be returned. If the number of available results is larger than `maxResults`, the response will provide a `nextPageToken` that can be used to get the next page of results in subsequent list requests. The server may return fewer than `maxResults` items even if there are more available. The client should check `nextPageToken` in the response to determine if there are more available. Must be non-negative. 0 will return no results but `nextPageToken` may be populated.<br><br>**pageToken** (type: String, optional): Specifies a page token to use. Set `pageToken` to the `nextPageToken` returned by a previous list request to get the next page of results. `nextPageToken` will not be returned in a response if there are no more results available.

<details open>
<summary><b>200: The shares were successfully returned.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json; charset=utf-8`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "items": [
    {
      "name": "string",
      "id": "string"
    }
  ],
  "nextPageToken": "string"
}
```

Note: the `items` field may be an empty array or missing when no results are found. The client must handle both cases.

Note: the `id` field is optional. If `id` is populated for a share, its value should be unique across the sharing server and immutable through the share's lifecycle. 

Note: the `nextPageToken` field may be an empty string or missing when there are no additional results. The client must handle both cases.
</td>
</tr>
</table>
</details>
<details>
<summary><b>400: The request is malformed.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>401: The request is unauthenticated. The bearer token is missing or incorrect.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>403: The request is forbidden from being fulfilled.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>500: The request is not handled correctly due to a server error.</b></summary>
<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>

Example:

`GET {prefix}/shares?maxResults=10&pageToken=...`

```json
{
   "items": [
      {
         "name": "vaccine_share",
         "id": "edacc4a7-6600-4fbb-85f3-a62a5ce6761f"   
      },
      {
         "name": "sales_share",
         "id": "3e979c79-6399-4dac-bcf8-54e268f48515"
      }
   ],
   "nextPageToken": "..."
}
```

### Get Share

This is the API to get the metadata of a share.

HTTP Request | Value
-- | --
Method | `GET`
Header | `Authorization: Bearer {token}`
URL | `{prefix}/shares/{share}`
URL Parameters | **{share}**: The share name to query. It's case-insensitive.

<details open>
<summary><b>200: The share's metadata was successfully returned.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json; charset=utf-8`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "share": {
    "name": "string",
    "id": "string"
  }
}
```

Note: the `id` field is optional. If `id` is populated for a share, its value should be unique across the sharing server and immutable through the share's lifecycle.

</td>
</tr>
</table>
</details>
<details>
<summary><b>400: The request is malformed.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>401: The request is unauthenticated. The bearer token is missing or incorrect.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>403: The request is forbidden from being fulfilled.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>404: The requested resource does not exist.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>500: The request is not handled correctly due to a server error.</b></summary>
<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>

Example:

`GET {prefix}/shares/vaccine_share`

```json
{
  "share": {
    "name": "vaccine_share",
    "id": "edacc4a7-6600-4fbb-85f3-a62a5ce6761f"
  }
}
```

### List Schemas in a Share

This is the API to list schemas in a share.

HTTP Request | Value
-|-
Method | `GET`
Header | `Authorization: Bearer {token}`
URL | `{prefix}/shares/{share}/schemas`
URL Parameters | **{share}**: The share name to query. It's case-insensitive.
Query Parameters | **maxResults** (type: Int32, optional): The maximum number of results per page that should be returned. If the number of available results is larger than `maxResults`, the response will provide a `nextPageToken` that can be used to get the next page of results in subsequent list requests. The server may return fewer than `maxResults` items even if there are more available. The client should check `nextPageToken` in the response to determine if there are more available. Must be non-negative. 0 will return no results but `nextPageToken` may be populated.<br><br>**pageToken** (type: String, optional): Specifies a page token to use. Set `pageToken` to the `nextPageToken` returned by a previous list request to get the next page of results. `nextPageToken` will not be returned in a response if there are no more results available.

<details open>
<summary><b>200: The schemas were successfully returned.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json; charset=utf-8`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "items": [
    {
      "name": "string",
      "share": "string"
    }
  ],
  "nextPageToken": "string",
}
```

Note: the `items` field may be an empty array or missing when no results are found. The client must handle both cases.

Note: the `nextPageToken` field may be an empty string or missing when there are no additional results. The client must handle both cases.
</td>
</tr>
</table>
</details>
<details>
<summary><b>400: The request is malformed.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>401: The request is unauthenticated. The bearer token is missing or incorrect.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>403: The request is forbidden from being fulfilled.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>404: The requested resource does not exist.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>500: The request is not handled correctly due to a server error.</b></summary>
<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>

Example:

`GET {prefix}/shares/vaccine_share/schemas?maxResults=10&pageToken=...`


```json
{
  "items": [
    {
      "name": "acme_vaccine_data",
      "share": "vaccine_share"
    }
  ],
  "nextPageToken": "..."
}
```

### List Tables in a Schema

This is the API to list tables in a schema.

HTTP Request | Value
-|-
Method | `GET`
Header | `Authorization: Bearer {token}`
URL | `{prefix}/shares/{share}/schemas/{schema}/tables`
URL Parameters | **{share}**: The share name to query. It's case-insensitive.<br>**{schema}**: The schema name to query. It's case-insensitive.
Query Parameters | **maxResults** (type: Int32, optional): The maximum number of results per page that should be returned. If the number of available results is larger than `maxResults`, the response will provide a `nextPageToken` that can be used to get the next page of results in subsequent list requests. The server may return fewer than `maxResults` items even if there are more available. The client should check `nextPageToken` in the response to determine if there are more available. Must be non-negative. 0 will return no results but `nextPageToken` may be populated.<br><br>**pageToken** (type: String, optional): Specifies a page token to use. Set `pageToken` to the `nextPageToken` returned by a previous list request to get the next page of results. `nextPageToken` will not be returned in a response if there are no more results available.

<details open>
<summary><b>200: The tables were successfully returned.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json; charset=utf-8`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "items": [
    {
      "name": "string",
      "schema": "string",
      "share": "string",
      "shareId": "string",
      "id": "string"
    }
  ],
  "nextPageToken": "string",
}
```

Note: the `items` field may be an empty array or missing when no results are found. The client must handle both cases.

Note: the `id` field is optional. If `id` is populated for a table, its value should be unique within the share it belongs to and immutable through the table's lifecycle.

Note: the `shareId` field is optional. If `shareId` is populated for a table, its value should be unique across the sharing server and immutable through the table's lifecycle.

Note: the `nextPageToken` field may be an empty string or missing when there are no additional results. The client must handle both cases.
</td>
</tr>
</table>
</details>
<details>
<summary><b>400: The request is malformed.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>401: The request is unauthenticated. The bearer token is missing or incorrect.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>403: The request is forbidden from being fulfilled.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>404: The requested resource does not exist.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>500: The request is not handled correctly due to a server error.</b></summary>
<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>

Example:

`GET {prefix}/shares/vaccine_share/schemas/acme_vaccine_data/tables?maxResults=10&pageToken=...`

```json
{
  "items" : [
    {
      "share" : "vaccine_share",
      "schema" : "acme_vaccine_data",
      "name" : "vaccine_ingredients",
      "shareId": "edacc4a7-6600-4fbb-85f3-a62a5ce6761f",
      "id": "dcb1e680-7da4-4041-9be8-88aff508d001"
    },
    {
      "share" : "vaccine_share",
      "schema" : "acme_vaccine_data",
      "name" : "vaccine_patients",
      "shareId": "edacc4a7-6600-4fbb-85f3-a62a5ce6761f",
      "id": "c48f3e19-2c29-4ea3-b6f7-3899e53338fa"
    }
  ],
  "nextPageToken" : "..."
}
```

### List all Tables in a Share

This is the API to list all the tables under all schemas in a share.

HTTP Request | Value
-|-
Method | `GET`
Header | `Authorization: Bearer {token}`
URL | `{prefix}/shares/{share}/all-tables`
URL Parameters | **{share}**: The share name to query. It's case-insensitive.
Query Parameters | **maxResults** (type: Int32, optional): The maximum number of results per page that should be returned. If the number of available results is larger than `maxResults`, the response will provide a `nextPageToken` that can be used to get the next page of results in subsequent list requests. The server may return fewer than `maxResults` items even if there are more available. The client should check `nextPageToken` in the response to determine if there are more available. Must be non-negative. 0 will return no results but `nextPageToken` may be populated.<br><br>**pageToken** (type: String, optional): Specifies a page token to use. Set `pageToken` to the `nextPageToken` returned by a previous list request to get the next page of results. `nextPageToken` will not be returned in a response if there are no more results available.

<details open>
<summary><b>200: The tables were successfully returned.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json; charset=utf-8`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "items": [
    {
      "name": "string",
      "schema": "string",
      "share": "string",
      "shareId": "string",
      "id": "string"
    }
  ],
  "nextPageToken": "string",
}
```

Note: the `items` field may be an empty array or missing when no results are found. The client must handle both cases.

Note: The `id` field is optional. If `id` is populated for a table, its value should be unique within the share it belongs to and immutable through the table's lifecycle.

Note: the `shareId` field is optional. If `shareId` is populated for a table, its value should be unique across the sharing server and immutable through the table's lifecycle.

Note: the `nextPageToken` field may be an empty string or missing when there are no additional results. The client must handle both cases.
</td>
</tr>
</table>
</details>
<details>
<summary><b>400: The request is malformed.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>401: The request is unauthenticated. The bearer token is missing or incorrect.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>403: The request is forbidden from being fulfilled.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>404: The requested resource does not exist.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>500: The request is not handled correctly due to a server error.</b></summary>
<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>

Example:

`GET {prefix}/shares/vaccine_share/all-tables`

```json
{
  "items" : [
    {
      "share" : "vaccine_share",
      "schema" : "acme_vaccine_ingredient_data",
      "name" : "vaccine_ingredients",
      "shareId": "edacc4a7-6600-4fbb-85f3-a62a5ce6761f",
      "id": "2f9729e9-6fcf-4d34-96df-bf72b26dfbe9"
    },
    {
      "share": "vaccine_share",
      "schema": "acme_vaccine_patient_data",
      "name": "vaccine_patients",
      "shareId": "edacc4a7-6600-4fbb-85f3-a62a5ce6761f",
      "id": "74be6365-0fc8-4a2f-8720-0de125bb5832"
    }
  ],
  "nextPageToken": "..."
}
```

### Query Table Version

This is the API for clients to get a table version without any other extra information. The server usually can implement this API effectively. If a client caches information about a shared table locally, it can store the table version and use this cheap API to quickly check whether their cache is stale and they should re-fetch the data.

HTTP Request | Value
-|-
Method | `HEAD`
Header | `Authorization: Bearer {token}`
URL | `{prefix}/shares/{share}/schemas/{schema}/tables/{table}`
URL Parameters | **{share}**: The share name to query. It's case-insensitive.<br>**{schema}**: The schema name to query. It's case-insensitive.<br>**{table}**: The table name to query. It's case-insensitive.

<details open>
<summary><b>200: The table version was successfully returned.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Delta-Table-Version: {version}`

**{version}** is a long value which represents the current table version.

</td>
</tr>
<tr>
<td>Body</td>
<td>

`Empty`

</td>

</td>
</tr>
</table>
</details>
<details>
<summary><b>400: The request is malformed.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>401: The request is unauthenticated. The bearer token is missing or incorrect.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>403: The request is forbidden from being fulfilled.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>404: The requested resource does not exist.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>500: The request is not handled correctly due to a server error.</b></summary>
<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>

Example:

`HEAD {prefix}/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients`

```
HTTP/2 200 
delta-table-version: 123
```

### Query Table Metadata

This is the API for clients to query the table schema and other metadata.

HTTP Request | Value
-|-
Method | `GET`
Header | `Authorization: Bearer {token}`
URL | `{prefix}/shares/{share}/schemas/{schema}/tables/{table}/metadata`
URL Parameters | **{share}**: The share name to query. It's case-insensitive.<br>**{schema}**: The schema name to query. It's case-insensitive.<br>**{table}**: The table name to query. It's case-insensitive.

<details open>
<summary><b>200: The table metadata was successfully returned.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Headers</td>
<td>

`Content-Type: application/x-ndjson; charset=utf-8`

`Delta-Table-Version: {version}`

**{version}** is a long value which represents the current table version.

</td>
</tr>
<tr>
<td>Body</td>
<td>

A sequence of JSON strings delimited by newline. Each line is a JSON object defined in [Table Metadata Format](#table-metadata-format).

The response contains two lines:
- The first line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Protocol](#protocol) object.
- The second line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Metadata](#metadata) object.

</td>
</tr>
</table>
</details>
<details>
<summary><b>400: The request is malformed.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>401: The request is unauthenticated. The bearer token is missing or incorrect.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>403: The request is forbidden from being fulfilled.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>404: The requested resource does not exist.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>500: The request is not handled correctly due to a server error.</b></summary>
<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>

Example (See [Table Metadata Format](#table-metadata-format) for more details about the format):

`GET {prefix}/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/metadata`

```
HTTP/2 200 
content-type: application/x-ndjson; charset=utf-8
delta-table-version: 123
```

```json
{
  "protocol": {
    "minReaderVersion": 1
  }
}
{
  "metaData": {
    "id": "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
    "format": {
      "provider": "parquet"
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": [
      "date"
    ]
  }
}
```

### Read Data from a Table

This is the API for clients to read data from a table.

<table>
<tr>
<th>HTTP Request</th>
<th>Value</th>
</tr>
<tr>
<td>Method</td>
<td>`POST`</td>
</tr>
<tr>
<td>Headers</td>
<td>

`Authorization: Bearer {token}`

`Content-Type: application/json; charset=utf-8`

</td>
</tr>
<tr>
<td>URL</td>
<td>

`{prefix}/shares/{share}/schemas/{schema}/tables/{table}/query`

</td>
</tr>
<tr>
<td>URL Parameters</td>
<td>

**{share}**: The share name to query. It's case-insensitive.

**{schema}**: The schema name to query. It's case-insensitive.

**{table}**: The table name to query. It's case-insensitive.
</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "predicateHints": [
    "string"
  ],
  "limitHint": 0
}

```

See [below](#request-body) for more details.
</td>
</tr>
</table>

<details open>
<summary><b>200: The tables were successfully returned.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Headers</td>
<td>

`Content-Type: application/x-ndjson; charset=utf-8`

`Delta-Table-Version: {version}`

**{version}** is a long value which represents the current table version.

</td>
</tr>
<tr>
<td>Body</td>
<td>

A sequence of JSON strings delimited by newline. Each line is a JSON object defined in [Table Metadata Format](#table-metadata-format).

The response contains multiple lines:
- The first line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Protocol](#protocol) object.
- The second line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Metadata](#metadata) object.
- The rest of the lines are [JSON wrapper objects](#json-wrapper-object-in-each-line) for [files](#file) in the table.

</td>
</tr>
</table>
</details>
<details>
<summary><b>400: The request is malformed.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>401: The request is unauthenticated. The bearer token is missing or incorrect.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>403: The request is forbidden from being fulfilled.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>404: The requested resource does not exist.</b></summary>

<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>
<details>
<summary><b>500: The request is not handled correctly due to a server error.</b></summary>
<table>
<tr>
<th>HTTP Response</th>
<th>Value</th>
</tr>
<tr>
<td>Header</td>
<td>

`Content-Type: application/json`

</td>
</tr>
<tr>
<td>Body</td>
<td>

```json
{
  "errorCode": "string",
  "message": "string"
}
```

</td>
</tr>
</table>
</details>


#### Request Body

The request body should be a JSON string containing the following two optional fields:
 
- **predicateHints** (type: String, optional): a list of SQL boolean expressions using [a restricted subset of SQL](#sql-expressions-for-filtering), in a JSON array.
  - When it’s present, the server will use the provided predicates as a hint to apply the SQL predicates on the returned files.
    - Filtering files based on the SQL predicates is **BEST EFFORT**. The server may return files that don’t satisfy the predicates.
    - If the server fails to parse one of the SQL predicates, or fails to evaluate it, the server may skip it.
    - Predicate expressions are conjunctive (AND-ed together).
  - When it’s absent, the server will return all of files in the table.

- **limitHint** (type: Int32, optional): an optional limit number. It’s a hint from the client to tell the server how many rows in the table the client plans to read. The server can use this hint to return only some files by using the file stats. For example, when running `SELECT * FROM table LIMIT 1000`, the client can set `limitHint` to `1000`.
  - Applying `limitHint` is **BEST EFFORT**. The server may return files containing more rows than the client requests.

When `predicateHints` and `limitHint` are both present, the server should apply `predicateHints` first then `limitHint`. As these two parameters are hints rather than enforcement, the client must always apply `predicateHints` and `limitHint` on the response returned by the server if it wishes to filter and limit the returned data. An empty JSON object (`{}`) should be provided when these two parameters are missing.

Example (See [Table Metadata Format](#table-metadata-format) for more details about the format):

`POST {prefix}/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/query`

```json
{
  "predicateHints": [
    "date >= '2021-01-01'",
    "date <= '2021-01-31'"
  ],
  "limitHint": 1000
}
```

```
HTTP/2 200 
content-type: application/x-ndjson; charset=utf-8
delta-table-version: 123
```

```json
{
  "protocol": {
    "minReaderVersion": 1
  }
}
{
  "metaData": {
    "id": "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
    "format": {
      "provider": "parquet"
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": [
      "date"
    ]
  }
}
{
  "file": {
    "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-8b0086f2-7b27-4935-ac5a-8ed6215a6640.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=97b6762cfd8e4d7e94b9d707eff3faf266974f6e7030095c1d4a66350cfd892e",
    "id": "8b0086f2-7b27-4935-ac5a-8ed6215a6640",
    "partitionValues": {
      "date": "2021-04-28"
    },
    "size":573,
    "stats": "{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"nullCount\":{\"eventTime\":0}}"
  }
}
{
  "file": {
    "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=899&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=0f7acecba5df7652457164533a58004936586186c56425d9d53c52db574f6b62",
    "id": "591723a8-6a27-4240-a90e-57426f4736d2",
    "partitionValues": {
      "date": "2021-04-28"
    },
    "size": 573,
    "stats": "{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}"
  }
}
```

## Table Metadata Format

This section discusses the table metadata format returned by the server.

### JSON Wrapper Object In Each Line

The JSON object in each line is a wrapper object that may contain the following fields:

Field Name | Data Type | Description | Optional/Required
-|-|-|-
protocol | The [Protocol](#protocol) JSON object. | Defines the versioning information about the table metadata format. | Optional
metaData | The [Metadata](#metadata) JSON object. | The table metadata including schema, partitionColumns, etc. | Optional
file | The [File](#file) JSON object. | An individual data file in the table. | Optional

It must contain only **ONE** of the above fields.

### Protocol

Protocol versioning will allow servers to exclude older clients that are missing features required to correctly interpret their response if the Delta Sharing Protocol evolves in the future. The protocol version will be increased whenever non-forward-compatible changes are made to the protocol. When a client is running an unsupported protocol version, it should show an error message instructing the user to upgrade to a newer version of their client.

Since only breaking changes must be accompanied by an increase in the protocol version recorded in a table, clients can assume that any unrecognized fields in a response that supports their reader version are never required in order to correctly interpret the response.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
minReaderVersion | Int32 | The minimum version of the protocol that a client must implement in order to correctly read a Delta Lake table. Currently it’s always `1`. It will be changed in future when we introduce non-forward-compatible changes that require clients to implement. | Required

Example (for illustration purposes; each JSON object must be a single line in the response):

```json
{
  "protocol": {
    "minReaderVersion": 1
  }
}
```

### Metadata

Field Name | Data Type | Description | Optional/Required
-|-|-|-
id | String | Unique identifier for this table | Required
name | String | User-provided identifier for this table | Optional
description | String | User-provided description for this table | Optional
format | [Format](#format) Object | Specification of the encoding for the files stored in the table. | Required
schemaString | String | Schema of the table. This is a serialized JSON string which can be deserialized to a [Schema](#schema-object) Object. | Required
partitionColumns | Array<String> | An array containing the names of columns by which the data should be partitioned. When a table doesn’t have partition columns, this will be an **empty** array. | Required

Example (for illustration purposes; each JSON object must be a single line in the response):

```json
{
  "metaData": {
    "partitionColumns": [
      "date"
    ],
    "format": {
      "provider": "parquet"
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}",
    "id": "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2"
  }
}
```

### File

Field Name | Data Type | Description | Optional/Required
-|-|-|-
url | String | A https url that a client can use to read the file directly. The same file in different responses may have different urls. | Required
id | String | A unique string for the file in a table. The same file is guaranteed to have the same id across multiple requests. A client may cache the file content and use this id as a key to decide whether to use the cached file content. | Required
partitionValues | Map<String, String> | A map from partition column to value for this file. See [Partition Value Serialization](#partition-value-serialization) for how to parse the partition values. When the table doesn’t have partition columns, this will be an **empty** map. | Required
size | Long | The size of this file in bytes. | Required
stats | String | Contains statistics (e.g., count, min/max values for columns) about the data in this file. This field may be missing. A file may or may not have stats. This is a serialized JSON string which can be deserialized to a [Statistics Struct](#per-file-statistics). A client can decide whether to use stats or drop it. | Optional

Example (for illustration purposes; each JSON object must be a single line in the response):

```json
{
  "file": {
    "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94",
    "id": "591723a8-6a27-4240-a90e-57426f4736d2",
    "size": 573,
    "partitionValues": {
      "date": "2021-04-28"
    },
    "stats": "{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}"
  }
}
```

### Format

Field Name | Data Type | Description | Optional/Required
-|-|-|-
provider | String | Name of the encoding for data files in this table. Currently the only valid value is `parquet`. This is to enable possible support for other formats in the future. | Required

### Schema Object

We use a subset of Spark SQL's JSON Schema representation to represent the schema of a table. A reference implementation can be found in [the Catalyst package of the Apache Spark repository](https://github.com/apache/spark/tree/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types).

#### Struct Type

A struct is used to represent both the top-level schema of the table as well as struct columns that contain nested columns. A struct is encoded as a JSON object with the following fields:

Field Name | Description
-|-
type | Always the string `struct`
fields | An array of fields

#### Struct Field

A struct field represents a top-level or nested column.

Field Name | Description
-|-
name | Name of this (possibly nested) column
type | String containing the name of a primitive type, a struct definition, an array definition or a map definition
nullable | Boolean denoting whether this field can be `null`
metadata | A JSON map containing information about this column. For example, the `comment` key means its value is the column comment.

#### Primitive Types

Field Name | Description
-|-
string | UTF-8 encoded string of characters
long | 8-byte signed integer. Range: `-9223372036854775808` to `9223372036854775807`
integer | 4-byte signed integer. Range: `-2147483648` to `2147483647`
short | 2-byte signed integer numbers. Range: `-32768` to `32767`
byte | 1-byte signed integer number. Range: `-128` to `127`
float | 4-byte single-precision floating-point numbers
double | 8-byte double-precision floating-point numbers
boolean | `true` or `false`
binary | A sequence of binary data
date | A calendar date, represented as a `year-month-day` triple without a timezone
timestamp | Microsecond precision timestamp without a timezone

#### Array Type

An array stores a variable length collection of items of some type.

Field Name | Description
-|-
type | Always the string `array`
elementType | The type of element stored in this array represented as a string containing the name of a primitive type, a struct definition, an array definition or a map definition
containsNull | Boolean denoting whether this array can contain one or more null values

#### Map Type

A map stores an arbitrary length collection of key-value pairs with a single keyType and a single valueType.

Field Name | Description
-|-
type | Always the string `map`
keyType | The type of element used for the key of this map, represented as a string containing the name of a primitive type, a struct definition, an array definition or a map definition
valueType | The type of element used for the key of this map, represented as a string containing the name of a primitive type, a struct definition, an array definition or a map definition
valueContainsNull | Indicates if map values have `null` values.

#### Example

Example Table Schema:

```
|-- a: integer (nullable = false)
|-- b: struct (nullable = true)
|    |-- d: integer (nullable = false)
|-- c: array (nullable = true)
|    |-- element: integer (containsNull = false)
|-- e: array (nullable = true)
|    |-- element: struct (containsNull = true)
|    |    |-- d: integer (nullable = false)
|-- f: map (nullable = true)
|    |-- key: string
|    |-- value: string (valueContainsNull = true)
```

JSON Encoded Table Schema:

```json
{
  "type": "struct",
  "fields": [
    {
      "name": "a",
      "type": "integer",
      "nullable": false,
      "metadata": {
        "comment": "this is a comment"
      }
    },
    {
      "name": "b",
      "type": {
        "type": "struct",
        "fields": [
          {
            "name": "d",
            "type": "integer",
            "nullable": false,
            "metadata": {}
          }
        ]
      },
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "c",
      "type": {
        "type": "array",
        "elementType": "integer",
        "containsNull": false
      },
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "e",
      "type": {
        "type": "array",
        "elementType": {
          "type": "struct",
          "fields": [
            {
              "name": "d",
              "type": "integer",
              "nullable": false,
              "metadata": {}
            }
          ]
        },
        "containsNull": true
      },
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "f",
      "type": {
        "type": "map",
        "keyType": "string",
        "valueType": "string",
        "valueContainsNull": true
      },
      "nullable": true,
      "metadata": {}
    }
  ]
}
```

### Partition Value Serialization

Partition values are stored as strings, using the following formats. An empty string for any type translates to a `null` partition value.

Type | Serialization Format
-|-
string | No translation required
numeric types | The string representation of the number
date | Encoded as `{year}-{month}-{day}`. For example, `1970-01-01`
timestamp | Encoded as `{year}-{month}-{day} {hour}:{minute}:{second}`. For example: `1970-01-01 00:00:00`
boolean | Encoded as the string `true` or `false`

### Per-file Statistics

A [File](#file) JSON object can optionally contain statistics about the data in the file being added to the table. These statistics can be used for eliminating files based on query predicates or as inputs to query optimization.

Global statistics record information about the entire file. The following global statistic is currently supported:

Name | Description
-|-
numRecords | The number of records in this file.

Per-column statistics record information for each column in the file and they are encoded, mirroring the schema of the actual data. For example, given the following data schema:

```
|-- a: struct
|    |-- b: struct
|    |    |-- c: long
```

Statistics could be stored with the following schema:

```
|-- stats: struct
|    |-- numRecords: long
|    |-- minValues: struct
|    |    |-- a: struct
|    |    |    |-- b: struct
|    |    |    |    |-- c: long
|    |-- maxValues: struct
|    |    |-- a: struct
|    |    |    |-- b: struct
|    |    |    |    |-- c: long
```

Example 1:

Column Type:

```
|-- a: long
```

JSON stats:

```json
{
   "numRecords": 1,
   "maxValues": {
      "a": 123
   },
   "minValues": {
      "a": 123
   },
   "nullCount": {
      "a": 0
   }
}
```


Example 2:

Column Type:

```
|-- a: struct
|    |-- b: struct
|    |    |-- c: long
```

JSON stats:

```json
{
   "minValues": {
      "a": {
         "b": {
            "c": 123
         }
      }
   },
   "maxValues": {
      "a": {
         "b": {
            "c": 123
         }
      }
   },
   "nullCount": {
      "a": {
         "b": {
            "c": 0
         }
      }
   },
   "numRecords": 1
}
```

The following per-column statistics are currently supported:

Name | Description
-|-
nullCount | The number of `null` values for this column
minValues | A value smaller than all values present in the file for this column
maxValues | A value larger than all values present in the file for this column

## SQL Expressions for Filtering

The client may send a sequence of predicates to the server as a hint to request fewer files when it only wishes to query a subset of the data (e.g., data where the `country` field is `US`). The server may try its best to filter files based on the predicates. This is **BEST EFFORT**, so the server may return files that don’t satisfy the predicates. For example, if the server fails to parse a SQL expression, the server can skip it. Hence, the client should always apply predicates to filter the data returned by the server.

At the beginning, the server will support the following simple comparison expressions between a column in the table and a constant value.

Expression | Example
-|-
`=` | `col = 123`
`>` | `col > 'foo'`
`<` | `'foo' < col`
`>=` | `col >= 123`
`<=` | `123 <= col`
`<>` | `col <> 'foo'`
`IS NULL` | `col IS NULL`
`IS NOT NULL` | `col IS NOT NULL`

More expression support will be added incrementally in future.

# Profile File Format

A profile file is a JSON file that contains the information for a recipient to access shared data on a Delta Sharing server. There are a few fields in this file as listed below.

Field Name | Descrption
-|-
shareCredentialsVersion | The file format version of the profile file. This version will be increased whenever non-forward-compatible changes are made to the profile format. When a client is running an unsupported profile file format version, it should show an error message instructing the user to upgrade to a newer version of their client.
endpoint | The url of the sharing server.
bearerToken | The [bearer token](https://tools.ietf.org/html/rfc6750) to access the server.
expirationTime | The expiration time of the bearer token in [ISO 8601 format](https://www.w3.org/TR/NOTE-datetime). This field is optional and if it is not provided, the bearer token can be seen as never expire.

Example:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://sharing.delta.io/delta-sharing/",
  "bearerToken": "<token>",
  "expirationTime": "2021-11-12T00:12:29.0Z"
}
```
