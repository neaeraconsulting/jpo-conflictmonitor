# http-tests

To run the HTTP tests:

Set the env vars and run the app locally via:

```bash
export CM_ATSPM_CLIENT_BASE_URL=...
export CM_ATSPM_CLIENT_USERNAME=...
export CM_ATSPM_CLIENT_PASSWORD=...
export CM_TEST_CONTROLLER_ENABLED=true
./mvnw spring-boot:run
```

`CM_TEST_CONTROLLER_ENABLED` is required - these `/test/**` endpoints are disabled by
default (they're unauthenticated and `/test/token` returns the live ATSPM access token),
so only set this for local development/testing, never in a shared or production
environment.

or run it in Docker.

Use Intellij HTTP Client or the VSCode REST Client to run the tests.

Run the `token.http` test first to initialize the service with a token.