# Authentication

By default, Whitefox runs with authentication disabled.

## Token authentication

We provide a simple way to secure your application using a single static token.

To enable token authentication you need to:

- set `whitefox.server.authentication.enabled` to `true`
- set `whitefox.server.authentication.bearerToken` to a secret string

After doing so, you need to provide a http header such as `Authentication: Bearer $token` in every request otherwise
you will receive a 401 error authentication error on the client.

| property name                              | default value   | description                                     |
|--------------------------------------------|-----------------|-------------------------------------------------|
| whitefox.server.authentication.enabled     | false           | either true or false to enable authentication   |
| whitefox.server.authentication.bearerToken |                 | the token used for authentication               |



In order to set configurations refer to [Quarkus documentation](https://quarkus.io/guides/config-reference).