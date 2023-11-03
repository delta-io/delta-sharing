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
    - [Read Change Data Feed from a Table](#read-change-data-feed-from-a-table)
  - [Delta Sharing Capabilities Header](#delta-sharing-capabilities-header)
  - [API Response Format in Parquet](#api-response-format-in-parquet)
    - [JSON Wrapper Object In Each Line](#json-wrapper-object-in-each-line)
    - [Protocol](#protocol)
    - [Metadata](#metadata)
    - [File](#file)
    - [Data Change Files](#data-change-files)
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
  - [API Response Format in Delta](#api-response-format-in-delta)
    - [JSON Wrapper Object In Each Line In Delta](#json-wrapper-object-in-each-line-in-delta)
    - [Protocol in Delta Format](#protocol-in-delta-format)
    - [Metadata in Delta Format](#metadata-in-delta-format)
    - [File in Delta Format](#file-in-delta-format)
  - [SQL Expressions for Filtering](#sql-expressions-for-filtering)
  - [JSON predicates for Filtering](#json-predicates-for-filtering)
  - [Delta Sharing Streaming Specs](#delta-sharing-streaming-specs)
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

Note: the `id` field is optional. If `id` is populated for a share, its value should be unique across the sharing server and stay immutable through the share's lifecycle. The format recommendation of `id` is UUID.

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

Note: the `id` field is optional. If `id` is populated for a share, its value should be unique across the sharing server and stay immutable through the share's lifecycle. The format recommendation of `id` is UUID.

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
  "nextPageToken": "string"
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
  "nextPageToken": "string"
}
```

Note: the `items` field may be an empty array or missing when no results are found. The client must handle both cases.

Note: the `id` field is optional. If `id` is populated for a table, its value should be unique within the share and stay immutable through the table's lifecycle. The format recommendation of `id` is UUID.

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
  "nextPageToken": "string"
}
```

Note: the `items` field may be an empty array or missing when no results are found. The client must handle both cases.

Note: the `id` field is optional. If `id` is populated for a table, its value should be unique within the share and stay immutable through the table's lifecycle. The format recommendation of `id` is UUID.

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

Note: This method is migrating from HEAD to GET, and with a `/version` suffix. Please use the new API as we encourage that the HEAD API be deprecated by 07/01/2023. 

<table>
<tr>
<th>HTTP Request</th>
<th colspan="2">Value</th>
</tr>
<tr>
<td>Method</td>
<td>GET</td>
<td>HEAD (deprecated)</td>
</tr>
<tr>
<td>HEADER</td>
<td colspan="2">

`Authorization: Bearer {token}`</td>
</tr>
<tr>
<td>URL</td>
<td>

`{prefix}/shares/{share}/schemas/{schema}/tables/{table}/version`
</td>
<td>

`{prefix}/shares/{share}/schemas/{schema}/tables/{table}`
</td>
</tr>
<tr>
<td>URL parameters</td>
<td colspan="2">

**{share}**: The share name to query. It's case-insensitive.<br>**{schema}**: The schema name to query. It's case-insensitive.<br>**{table}**: The table name to query. It's case-insensitive.
</td>
</tr>
<tr>
<td>Query parameters</td>
<td>

**startingTimestamp** (type: String, optional): The startingTimestamp of the query, a string in the [Timestamp Format](#timestamp-format), the server needs to return the earliest table version at or after the provided timestamp, can be earlier than the timestamp of table version 0.
</td>
<td>N/A</td>
</tr>
</table>

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

`GET {prefix}/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/version`

```
HTTP/2 200 
delta-table-version: 123
```

### Query Table Metadata

This is the API for clients to query the table schema and other metadata.

<table>
<tr>
<th>HTTP Request</th>
<th>Value</th>
</tr>
<tr>
<td>Method</td>
<td>

`GET`

</td>
</tr>
<tr>
<td>Headers</td>
<td>

`Authorization: Bearer {token}`

Optional: `delta-sharing-capabilities: responseformat=delta;readerfeatures=deletionvectors`, see 
[Delta Sharing Capabilities Header](#delta-sharing-capabilities-header) for details. 

</td>
</tr>
<tr>
<td>URL</td>
<td>

`{prefix}/shares/{share}/schemas/{schema}/tables/{table}/metadata`

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
</table>

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

A sequence of JSON strings delimited by newline.

When `responseformat=parquet`, each line is a JSON object defined in [API Response Format in Parquet](#api-response-format-in-parquet).

The response contains two lines:
- The first line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Protocol](#protocol) object.
- The second line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Metadata](#metadata) object.

When `responseformat=delta`, each line is a Json object defined in [API Response Format in Delta](#api-response-format-in-delta).
The response contains two lines:
- The first line is [a JSON wrapper object](#json-wrapper-object-in-each-line-in-delta) containing the delta [Protocol](#protocol-in-delta-format) object.
- The second line is [a JSON wrapper object](#json-wrapper-object-in-each-line-in-delta) containing the delta [Metadata](#metadata-in-delta-format) object.

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

Example (See [API Response Format in Parquet](#api-response-format-in-parquet) for more details about the format):

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
<td>

`POST`

</td>
</tr>
<tr>
<td>Headers</td>
<td>

`Authorization: Bearer {token}`

Optional: `Content-Type: application/json; charset=utf-8`

Optional: `delta-sharing-capabilities: responseformat=delta;readerfeatures=deletionvectors`, see
[Delta Sharing Capabilities Header](#delta-sharing-capabilities-header) for details.

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
<td>Body (an example)</td>
<td>

```json
{
  "predicateHints": [
    "string"
  ],
  "limitHint": 0,
  "version": 0,
  "jsonPredicateHints": "string"
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

**{version}** is a long value:
- when no time travel parameters are provided in the request, it 
represents the current table version.
- otherwise, it represents the starting version of files 
returned in the response.

</td>
</tr>
<tr>
<td>Body</td>
<td>

When `responseformat=parquet`, a sequence of JSON strings delimited by newline. Each line is a JSON object defined in [API Response Format in Parquet](#api-response-format-in-parquet).

The response contains multiple lines:
- The first line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Protocol](#protocol) object.
- The second line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Metadata](#metadata) object.
- The rest of the lines are [JSON wrapper objects](#json-wrapper-object-in-each-line) for [data change files](#data-change-files), [Metadata](#metadata), or [files](#file).  
  - The lines are [data change files](#data-change-files) with possible historical [Metadata](#metadata) (when startingVersion is set).
  - The lines are [files](#file) in the table (otherwise).
  - The ordering of the lines doesn't matter.

When `responseformat=delta`, a sequence of JSON strings delimited by newline. Each line is a JSON object defined in [API Response Format in Delta](#api-response-format-in-delta).

The response contains multiple lines:
- The first line is [a JSON wrapper object](#json-wrapper-object-in-each-line-in-delta) containing the delta [Protocol](#protocol-in-delta-format) object.
- The second line is [a JSON wrapper object](#json-wrapper-object-in-each-line-in-delta) containing the delta [Metadata](#metadata-in-delta-format) object.
- The rest of the lines are [JSON wrapper objects](#json-wrapper-object-in-each-line-in-delta) for [Metadata](#metadata-in-delta-format), or [files](#file-in-delta-format).
  - The lines are [files](#file-in-delta-format) which wraps the delta single action  in the table (otherwise), with possible historical [Metadata](#metadata-in-delta-format) (when startingVersion is set).
  - The ordering of the lines doesn't matter.

The delta actions are wrapped because they will be used to construct a local delta log on the recipient
side and then leverage the delta library to read data. 

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

The request body should be a JSON string containing the following optional fields:
 
- **predicateHints** (type: String, optional): a list of SQL boolean expressions using [a restricted subset of SQL](#sql-expressions-for-filtering), in a JSON array.
  - When it’s present, the server will use the provided predicates as a hint to apply the SQL predicates on the returned files.
    - Filtering files based on the SQL predicates is **BEST EFFORT**. The server may return files that don’t satisfy the predicates.
    - If the server fails to parse one of the SQL predicates, or fails to evaluate it, the server may skip it.
    - Predicate expressions are conjunctive (AND-ed together).
  - When it’s absent, the server will return all of files in the table.
  - **This will be deprecated once all the client and server implementation move to using jsonPredicateHints below**.

- **jsonPredicateHints** (type: String, optional): query predicates on partition columns specified using [a structured JSON format](#json-predicates-for-filtering).
  - When it’s present, the server will try to use the predicates to filter table's files, which could boost query performance.
    - As with **predicateHints**, this filtering is **BEST EFFORT**. The server may return files that don’t satisfy the predicates.
    - If the server encounters any errors during predicate processing (for example, invalid syntax or non existing columns), it will skip filtering and return all the files. 
  - When it’s absent, the server will return all the files in the table.

- **limitHint** (type: Int32, optional): an optional limit number. It’s a hint from the client to tell the server how many rows in the table the client plans to read. The server can use this hint to return only some files by using the file stats. For example, when running `SELECT * FROM table LIMIT 1000`, the client can set `limitHint` to `1000`.
  - Applying `limitHint` is **BEST EFFORT**. The server may return files containing more rows than the client requests.

- **version** (type: Long, optional): an optional version number. If set, will return files as of the specified version of the table. This is only supported on tables with history sharing enabled.

- **timestamp** (type: String, optional): an optional timestamp string in the [Timestamp Format](#timestamp-format),. If set, will return files as of the table version corresponding to the specified timestamp. This is only supported on tables with history sharing enabled.

- **startingVersion** (type: Long, optional): an optional version number. If set, will return all data change files since startingVersion, inclusive, including historical metadata if seen in the delta log.

- **endingVersion** (type: Long, optional): an optional version number, only used if startingVersion is set. If set, the server can use it as a hint to avoid returning data change files after `endingVersion`. This is not enforcement. Hence, when sending the `endingVersion` parameter, the client should still handle the case that it may receive files after `endingVersion`.
  - The combination of `statingVersion` and `endingVersion` can be used as query window for delta sharing streaming rpcs.

When `predicateHints` and `limitHint` are both present, the server should apply `predicateHints` first then `limitHint`. As these two parameters are hints rather than enforcement, the client must always apply `predicateHints` and `limitHint` on the response returned by the server if it wishes to filter and limit the returned data. An empty JSON object (`{}`) should be provided when these two parameters are missing.

Example (See [API Response Format in Parquet](#api-response-format-in-parquet) for more details about the format):

`POST {prefix}/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/query`

```json
{
  "predicateHints": [
    "date >= '2021-01-01'",
    "date <= '2021-01-31'"
  ],
  "limitHint": 1000,
  "version": 123
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

### Read Change Data Feed from a Table
This is the API for clients to read change data feed from a table.

The API supports a start parameter and and an end parameter. The start/end parameter can either be a version or a timestamp. The start parameter must be provided. If the end parameter is not provided, the API will use the latest table version for it. The parameter range is inclusive in the query.

You can specify a version as a Long or a timestamp as a string in the [Timestamp Format](#timestamp-format).

The change data feed represents row-level changes between versions of a Delta table. It records change data for UPDATE, DELETE, and MERGE operations. If you leverage the connectors provided by this library to read change data feed, it results in three metadata columns that identify the type of change event, in addition to the data columns:
- _change_type (type: String): There are four values: insert, update_preimage, update_postimage, delete. preimage is the value before the udpate, postimage is the value after the update.
- _commit_version (type: Long): The table version containing the change.
- _commit_timestamp (type: Long): The unix timestamp associated when the commit of the change was created, in milliseconds. 

<table>
<tr>
<th>HTTP Request</th>
<th>Value</th>
</tr>
<tr>
<td>Method</td>
<td>
 
`GET`
 
</td>
</tr>
<tr>
<td>Header</td>
<td>

`Authorization: Bearer {token}`

Optional: `delta-sharing-capabilities: responseformat=delta;readerfeatures=deletionvectors`, see
[Delta Sharing Capabilities Header](#delta-sharing-capabilities-header) for details.

</td>
</tr>
<tr>
<td>URL</td>
<td>

`{prefix}/shares/{share}/schemas/{schema}/tables/{table}/changes`

</td>
</tr>
<tr>
<td>URL Parameters</td>
<td>

**{share}**: The share name to query. It's case-insensitive.<br>
**{schema}**: The schema name to query. It's case-insensitive.<br>
**{table}**: The table name to query. It's case-insensitive.
</td>
</tr>
<tr>
<td>Query Parameters</td>
<td>

 **startingVersion** (type: Long, optional): The starting version of the query, inclusive. <br>
 **startingTimestamp** (type: String, optional): The starting timestamp of the query, a string in the [Timestamp Format](#timestamp-format), which will be converted to a version created greater or equal to this timestamp. <br>
 **endingVersion** (type: Long, optional): The ending version of the query, inclusive. <br>
 **endingTimestamp** (type: String, optional): The ending timestamp of the query, a string in the [Timestamp Format](#timestamp-format), which will be converted to a version created earlier than or at the timestamp. <br>
 **includeHistoricalMetadata** (type: Boolean, optional): If set to true, return the historical metadata if seen in the delta log. This is for the streaming client to check if the table schema is still read compatible.</td>
</tr>
</table>

<details open>
<summary><b>200: The change data feed was successfully returned.</b></summary>

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

**{version}** is a long value which represents the starting version of files in the response.

</td>
</tr>
<tr>
<td>Body</td>
<td>

When `responseformat=parquet`, a sequence of JSON strings delimited by newline. Each line is a JSON object defined in [API Response Format in Parquet](#api-response-format-in-parquet).

The response contains multiple lines:
- The first line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Protocol](#protocol) object.
- The second line is [a JSON wrapper object](#json-wrapper-object-in-each-line) containing the table [Metadata](#metadata) object.
- The rest of the lines are [JSON wrapper objects](#json-wrapper-object-in-each-line) for [Data Change Files](#data-change-files) of the change data feed.
  - Historical [Metadata](#metadata) will be returned if includeHistoricalMetadata is set to true.
  - The ordering of the lines doesn't matter.

When `responseformat=delta`, a sequence of JSON strings delimited by newline. Each line is a JSON object defined in [API Response Format in Parquet](#api-response-format-in-delta).
- The first line is [a JSON wrapper object](#json-wrapper-object-in-each-line-in-delta) containing the delta [Protocol](#protocol-in-delta-format) object.
- The second line is [a JSON wrapper object](#json-wrapper-object-in-each-line-in-delta) containing the delta [Metadata](#metadata-in-delta-format) object.
- The rest of the lines are [JSON wrapper objects](#json-wrapper-object-in-each-line) for [Files](#file-in-delta-format) of the change data feed.
  - Historical [Metadata](#metadata) will be returned if includeHistoricalMetadata is set to true.
  - The ordering of the lines doesn't matter.

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

Example (See [API Response Format in Parquet](#api-response-format-in-parquet) for more details about the format):

`GET {prefix}/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/changes?startingVersion=0&endingVersion=2`


```
HTTP/2 200 
content-type: application/x-ndjson; charset=utf-8
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
    ],
    "configuration": {
      "enableChangeDataFeed": "true"
    }
  }
}
{
  "add": {
    "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/date%3D2021-04-28/part-00000-8b0086f2-7b27-4935-ac5a-8ed6215a6640.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=97b6762cfd8e4d7e94b9d707eff3faf266974f6e7030095c1d4a66350cfd892e",
    "id": "8b0086f2-7b27-4935-ac5a-8ed6215a6640",
    "partitionValues": {
      "date": "2021-04-28"
    },
    "size":573,
    "stats": "{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"nullCount\":{\"eventTime\":0}}",
    "timestamp": 1652140000000,
    "version": 0
  }
}
{
  "cdf": {
    "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/_change_data/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=899&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=0f7acecba5df7652457164533a58004936586186c56425d9d53c52db574f6b62",
    "id": "591723a8-6a27-4240-a90e-57426f4736d2",
    "partitionValues": {
      "date": "2021-04-28"
    },
    "size": 689,
    "timestamp": 1652141000000,
    "version": 1
  }
}
{
  "remove": {
    "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/date%3D2021-04-28/part-00000-8b0086f2-7b27-4935-ac5a-8ed6215a6640.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=97b6762cfd8e4d7e94b9d707eff3faf266974f6e7030095c1d4a66350cfd892e",
    "id": "8b0086f2-7b27-4935-ac5a-8ed6215a6640",
    "partitionValues": {
      "date": "2021-04-28"
    },
    "size": 573,
    "timestamp": 1652142000000,
    "version": 2
  }
}
```

### Timestamp Format
Accepted timestamp format by a delta sharing server: in the ISO8601 format, in the UTC timezone, such as `2022-01-01T00:00:00Z`.   

## Delta Sharing Capabilities Header
This section explains the details of delta sharing capabilities header, which was introduced to help 
delta sharing catch up with features in [delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

The key of the header is **delta-sharing-capabilities**, the value is semicolon separated capabilities. 
Each capability is in the format of "key=value1,value2", values are separated by commas.
Example: "responseformat=delta;readerfeatures=deletionvectors,columnmapping". All keys and values should
be case-insensitive when processed by the server.

This header can be used in the request for [Query Table Metadata](#query-table-metadata), 
[Query Table](#read-data-from-a-table), and [Query Table Changes](#read-change-data-feed-from-a-table).

**Compatibility**

<table>
<tr>
<th>Client/Server</th>
<th>Server that doesn't recognize the header</th>
<th>Server that recognizes the header</th>
</tr>
<tr>
<th>Client that doesn't specify the header</th>
<td>Response is in parquet format</td>
<td>Response must be in parquet format.</td>
</tr>
<tr>
<th>Client that specifies the header</th>
<td>The header is ignored at the server, and the format of the response must always be parquet.
</td>
<td>The header is processed properly by the server.

If there's only one responseFormat specified, the server must respect and return in the requested format.  

If there's a list of responseFormat specified, such as `responseFormat=delta,parquet`. The server 
may choose to respond in parquet format if the table does not have any advanced features. The server
must respond in delta format if the table has advanced features which are not compatible with the parquet format.
</td>
</tr>
</table>

- If the client requests `delta` format and the response is in `parquet` format, the delta sharing
client will NOT throw an error. Ideally, the caller of the client's method should handle such 
responses to be compatible with legacy servers.
- If the client doesn't specify any header, or requests `parquet` format and the response is in 
`delta` format, the delta sharing client must throw an error.

### responseFormat
Indicates the format to expect in the [API Response Format in Parquet](#api-response-format-in-parquet), two values are supported.

- parquet: Represents the format of the delta sharing protocol that has been used in `delta-sharing-spark` 1.0 
and less, also the default format if `responseFormat` is missing from the header. All the existing delta
sharing connectors are able to process data in this format. 
- **delta**: format can be used to read a shared delta table with minReaderVersion > 1, which contains 
readerFeatures such as Deletion Vector or Column Mapping. `delta-sharing-spark` libraries 
that are able to process `responseformat=delta` will be released soon.

### readerFeatures
readerfeatures is only useful when `responseformat=delta`, it includes values from [delta reader
features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-features). It's set by the
caller of `DeltaSharingClient` to indicate its ability to process delta readerFeatures.

## API Response Format in Parquet

This section discusses the API Response Format in Parquet returned by the server.

### JSON Wrapper Object In Each Line

The JSON object in each line is a wrapper object that may contain the following fields:

Field Name | Data Type | Description | Optional/Required
-|-|-|-
protocol | The [Protocol](#protocol) JSON object. | Defines the versioning information about the API Response Format in Parquet. | Optional
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
configuration | Map[String, String] | A map containing configuration options for the table | Optional
version | Long | The table version the metadata corresponds to, returned when querying table data with a version or timestamp parameter, or cdf query with includeHistoricalMetadata set to true. | Optional
size | Long | The size of the table in bytes, will be returned if available in the delta log. | Optional 
numFiles | Long | The number of files in the table, will be returned if available in the delta log. | Optional

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
    "id": "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
    "configuration": {
      "enableChangeDataFeed": "true"
    },
    "size": 123456,
    "numFiles": 5
  }
}
```

### File

Field Name | Data Type | Description | Optional/Required
-|-|-|-
url | String | An https url that a client can use to read the file directly. The same file in different responses may have different urls. | Required
id | String | A unique string for the file in a table. The same file is guaranteed to have the same id across multiple requests. A client may cache the file content and use this id as a key to decide whether to use the cached file content. | Required
partitionValues | Map<String, String> | A map from partition column to value for this file. See [Partition Value Serialization](#partition-value-serialization) for how to parse the partition values. When the table doesn’t have partition columns, this will be an **empty** map. | Required
size | Long | The size of this file in bytes. | Required
stats | String | Contains statistics (e.g., count, min/max values for columns) about the data in this file. This field may be missing. A file may or may not have stats. This is a serialized JSON string which can be deserialized to a [Statistics Struct](#per-file-statistics). A client can decide whether to use stats or drop it. | Optional
version | Long | The table version of the file, returned when querying a table data with a version or timestamp parameter. | Optional
timestamp | Long | The unix timestamp corresponding to the table version of the file, in milliseconds, returned when querying a table data with a version or timestamp parameter. | Optional
expirationTimestamp | Long | The unix timestamp corresponding to the expiration of the url, in milliseconds, returned when the server supports the feature. | Optional

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
    "stats": "{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}",
    "expirationTimestamp": 1652140800000
  }
}
```

### Data Change Files

#### Add File
Field Name | Data Type | Description | Optional/Required
-|-|-|-
url | String | An https url that a client can use to read the file directly. The same file in different responses may have different urls. | Required
id | String | A unique string for the file in a table. The same file is guaranteed to have the same id across multiple requests. A client may cache the file content and use this id as a key to decide whether to use the cached file content. | Required
partitionValues | Map<String, String> | A map from partition column to value for this file. See [Partition Value Serialization](#partition-value-serialization) for how to parse the partition values. When the table doesn’t have partition columns, this will be an **empty** map. | Required
size | Long | The size of this file in bytes. | Required
timestamp | Long | The timestamp of the file in milliseconds from epoch. | Required
version | Int32 | The table version of this file. | Required
stats | String | Contains statistics (e.g., count, min/max values for columns) about the data in this file. This field may be missing. A file may or may not have stats. This is a serialized JSON string which can be deserialized to a [Statistics Struct](#per-file-statistics). A client can decide whether to use stats or drop it. | Optional
expirationTimestamp | Long | The unix timestamp corresponding to the expiration of the url, in milliseconds, returned when the server supports the feature. | Optional

Example (for illustration purposes; each JSON object must be a single line in the response):

```json
{
  "add": {
    "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94",
    "id": "591723a8-6a27-4240-a90e-57426f4736d2",
    "size": 573,
    "partitionValues": {
      "date": "2021-04-28"
    },
    "timestamp": 1652140800000,
    "version": 1,
    "stats": "{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}",
    "expirationTimestamp": 1652144400000
  }
}
```

#### CDF File
Field Name | Data Type | Description | Optional/Required
-|-|-|-
url | String | An https url that a client can use to read the file directly. The same file in different responses may have different urls. | Required
id | String | A unique string for the file in a table. The same file is guaranteed to have the same id across multiple requests. A client may cache the file content and use this id as a key to decide whether to use the cached file content. | Required
partitionValues | Map<String, String> | A map from partition column to value for this file. See [Partition Value Serialization](#partition-value-serialization) for how to parse the partition values. When the table doesn’t have partition columns, this will be an **empty** map. | Required
size | Long | The size of this file in bytes. | Required
timestamp | Long | The timestamp of the file in milliseconds from epoch. | Required
version | Int32 | The table version of this file. | Required
expirationTimestamp | Long | The unix timestamp corresponding to the expiration of the url, in milliseconds, returned when the server supports the feature. | Optional

Example (for illustration purposes; each JSON object must be a single line in the response):

```json
{
  "cdf": {
    "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/_change_data/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94",
    "id": "591723a8-6a27-4240-a90e-57426f4736d2",
    "size": 573,
    "partitionValues": {
      "date": "2021-04-28"
    },
    "timestamp": 1652140800000,
    "version": 1,
    "expirationTimestamp": 1652144400000
  }
}
```

#### Remove File
Field Name | Data Type | Description | Optional/Required
-|-|-|-
url | String | An https url that a client can use to read the file directly. The same file in different responses may have different urls. | Required
id | String | A unique string for the file in a table. The same file is guaranteed to have the same id across multiple requests. A client may cache the file content and use this id as a key to decide whether to use the cached file content. | Required
partitionValues | Map<String, String> | A map from partition column to value for this file. See [Partition Value Serialization](#partition-value-serialization) for how to parse the partition values. When the table doesn’t have partition columns, this will be an **empty** map. | Required
size | Long | The size of this file in bytes. | Required
timestamp | Long | The timestamp of the file in milliseconds from epoch. | Required
version | Int32 | The table version of this file. | Required
expirationTimestamp | Long | The unix timestamp corresponding to the expiration of the url, in milliseconds, returned when the server supports the feature. | Optional

Example (for illustration purposes; each JSON object must be a single line in the response):

```json
{
  "remove": {
    "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94",
    "id": "591723a8-6a27-4240-a90e-57426f4736d2",
    "size": 573,
    "partitionValues": {
      "date": "2021-04-28"
    },
    "timestamp": 1652140800000,
    "version": 1,
    "expirationTimestamp": 1652144400000
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
decimal | signed decimal number with fixed precision (maximum number of digits) and scale (number of digits on right side of dot). The precision and scale can be up to 38.

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

## API Response Format in Delta
This section discusses the API Response Format in Delta returned by the server. When a table is shared
as delta format, the actions in the response could be put in a delta log in the local storage on the
recipient side for the delta library to read data out of it directly. This way of sharing makes the
delta sharing protocol more transparent and robust in supporting advanced delta feature, and minimizes code duplication.

### JSON Wrapper Object In Each Line in Delta

The JSON object in each line is a wrapper object that may contain the following fields. For each
field, it is a wrapper of a [delta action](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions)(which keeps the action in its delta format and with original
values), and with some additional fields for delta sharing functionalities.

Field Name | Data Type | Description | Optional/Required
-|-|-|-
protocol | The [Protocol in Delta Format](#protocol-in-delta-format) JSON object. | A wrapper of delta protocol. | Optional
metaData | The [Metadata in Delta Format](#metadata-in-delta-format) JSON object. | A wrapper of delta metadata, including some delta sharing specific fields. | Optional
file | The [File in Delta Format](#file-in-delta-format) JSON object. | A wrapper of a delta single action in the table. | Optional

It must contain only **ONE** of the above fields.

### Protocol in Delta Format

A wrapper of a [delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#protocol-evolution).

Field Name | Data Type | Description | Optional/Required
-|-|-|-
deltaProtocol | Delta Protocol | Need to be parsed by a delta library as a delta protocol. | Required

Example (for illustration purposes; each JSON object must be a single line in the response):

```json
{
  "protocol": {
    "deltaProtocol": {
      "minReaderVersion": 3,
      "minWriterVersion": 7
    }
  }
}
```

### Metadata in Delta Format

A wrapper of a [delta Metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata).

Field Name | Data Type | Description | Optional/Required
-|-|-|-
deltaMetadata | Delta Metadata | Need to be parsed by a delta library as delta metadata | Required
version | Long | The table version the metadata corresponds to, returned when querying table data with a version or timestamp parameter, or cdf query with includeHistoricalMetadata set to true. | Optional
size | Long | The size of the table in bytes, will be returned if available in the delta log. | Optional
numFiles | Long | The number of files in the table, will be returned if available in the delta log. | Optional

Example (for illustration purposes; each JSON object must be a single line in the response):

```json
{
  "metaData": {
    "version": 20,
    "size": 123456,
    "numFiles": 5,
    "deltaMetadata": {
      "partitionColumns": [
        "date"
      ],
      "format": {
        "provider": "parquet"
      },
      "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}",
      "id": "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
      "configuration": {
        "enableChangeDataFeed": "true"
      }
    }
  }
}
```

### File in Delta Format

A wrapper of a delta file action, which can be [Add File and Remove File](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file),
or [Add CDC File](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file)

Field Name | Data Type | Description | Optional/Required
-|-|-|-
id | String | A unique string for the file in a table. The same file is guaranteed to have the same id across multiple requests. A client may cache the file content and use this id as a key to decide whether to use the cached file content. | Required
deletionVectorFileId | String | A unique string for the deletion vector file in a table. The same deletion vector file is guaranteed to have the same id across multiple requests. A client may cache the file content and use this id as a key to decide whether to use the cached file content. | Optional
version | Long | The table version of the file, returned when querying a table data with a version or timestamp parameter. | Optional
timestamp | Long | The unix timestamp corresponding to the table version of the file, in milliseconds, returned when querying a table data with a version or timestamp parameter. | Optional
expirationTimestamp | Long | The unix timestamp corresponding to the expiration of the url, in milliseconds, returned when the server supports the feature. | Optional
deltaSingleAction | Delta SingleAction | Need to be parsed by a delta library as a delta single action, the path field is replaced by pr-signed url. | Required 

Example (for illustration purposes; each JSON object must be a single line in the response):

```json
{
  "file": {
    "id": "591723a8-6a27-4240-a90e-57426f4736d2",
    "size": 573,
    "expirationTimestamp": 1652140800000,
    "deltaSingleAction": {
      "add": {
        "path": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?...",
        "partitionValues": {
          "date": "2021-04-28"
        },
        "stats": "{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}"
      }
    }
  }
}
```

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

## JSON predicates for Filtering

The client may send filtering predicates on partition columns using a structured JSON format. This is similar to [SQL expressions for Filtering](#sql-expressions-for-filtering), but uses a simpler JSON tree structure. The partition column predicates are simple to express, and do not need the full expressive power of SQL. Using a structured JSON format to represent these predicates enables easier and safer support for file filtering capabilities in Delta Sharing server implementations.

The server may try its best to filter files based on the predicates in a **BEST EFFORT** mode. The server may return all the files if validations fail. So clients should apply these predicates to the filter the data returned by the server.

We will deprecate the legacy predicateHints once all the delta sharing client and servers have moved to the json predicate based file filtering.

The JSON format:

Field Name | Descrption
-|-
op | The name of the operation. See the section on supported ops for details.
children | The child operations for the op. This allows us to represent a predicate tree. Leaf ops do not have any children. Unary ops such as **not** have only one child. Binary ops such as **equal** have two children. Nary ops such as **and** can have multiple children.
name | Specifies the name of a column. This is only applicable to column ops.
value | Specifies the value of a literal. This is only applicable to literal ops.
valueType | Specifies the value type of a column or a literal op. This is only applicate to column and literal ops.

The supported Ops:

Op | Op Type | Description
-|-|-
column | Leaf | Represents a column. A column op has two fields: **name** and **valueType**. The column's value will be cast to the specified **valueType** during comparisons.
literal | Leaf | Represents a literal or fixed value. A literal op has two fields: **value** and **valueType**. The literal's **value**  will be cast to the specified **valueType** during comparisons.
isNull | Unary | Represents a null check on a column op. This op should have only one child, the column op.
equal | Binary | Represents an equality ("=") check. This op should have two children, and both should be leaf ops.
lessThan | Binary | Represents a less than ("<") check. This op should have two children, and both should be leaf ops.
lessThanOrEqual | Binary | Represents a less than or equal ("<=") check. This op should have two children, and both should be leaf ops.
greaterThan | Binary | Represents a greater than (">") check. This op should have two children, and both should be leaf ops.
greaterThanOrEqual | Binary | Represents a greater than (">=") check. This op should have two children, and both should be leaf ops.
and | Nary | Represents a logical and operation amongst its children. This op should have at least two children.
or | Nary | Represents a logical or operation amongst its children. This op should have at least two children.
not | Unary | Represents a logical not check. This op should have once child.

The supported value types:

ValueType | Description
-|-
"bool" | Represents an Boolean type.
"int" | Represents an Integer type.
"long" | Represents a Long type.
"string" | Represents a String type.
"date" | Represents a Date type in "yyyy-mm-dd" format.
"float" | Represents a Float type.
"double" | Represents a Double type.
"timestamp" | Represents a timestamp in [Timestamp Format](#timestamp-format).


Examples


1. Equal op.

```json
{
  "op": "equal",
  "children": [
    {"op": "column", "name":"hireDate", "valueType":"date"},
    {"op":"literal","value":"2021-04-29","valueType":"date"}
  ]
}
```

2. And op.

```json
{
  "op":"and",
  "children":[
    {
      "op":"equal",
      "children":[
        {"op":"column","name":"hireDate","valueType":"date"},
        {"op":"literal","value":"2021-04-29","valueType":"date"}
      ]
    },
    {
      "op":"lessThan","children":[
        {"op":"column","name":"id","valueType":"int"},
        {"op":"literal","value":"25","valueType":"int"}
      ]
    }
  ]
}
```

3. Not null check.

```json
{
  "op": "not",
  "children": [
    {
      "op": "isNull",
      "children":[
        {"op":"column","name":"id","valueType":"int"},
      ]
    }
  ]
}
```

## Delta Sharing Streaming Specs
Delta Sharing Streaming is supported starting from delta-sharing-spark 0.6.0. As it's implemented
based on spark structured streaming, it leverages a pull model to consume the new data of the shared
table from the delta sharing server. In addition to most options supported in delta streaming,
there are two options/spark configs for delta sharing streaming.

- spark config **spark.delta.sharing.streaming.queryTableVersionIntervalSeconds**: DeltaSharingSource
leverages [getTableVersion](#query-table-version) rpc to check whether there is new data available
to consume. In order to reduce the traffic burden to the delta sharing server, there's a minimum 30
seconds interval between two getTableVersion rpcs to the delta sharing server. Though, if you are ok
with less freshness on the data and want to reduce the traffic to the server, you can set this 
config to a larger number, for example: 60s or 120s. An error will be thrown if it's set less than 30 seconds.
- option **maxVersionsPerRpc**: DeltaSharingSource leverages [QueryTable](#read-data-from-a-table)
rpc to continuously read new data from the delta sharing server. There might be too much
new data to be returned from the server if the streaming has paused for a while on the recipient
side. Its default value is 100, a smaller number is recommended such as `.option("maxVersionsPerRpc", 10)` 
to reduce the traffic load for each rpc. This shouldn't affect the freshness of the data significantly
assuming the process time of the delta sharing server grows linearly with `maxVersionsPerRpc`.

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
