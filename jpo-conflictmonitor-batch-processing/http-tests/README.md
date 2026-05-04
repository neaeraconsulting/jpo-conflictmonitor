# http-tests

To run the HTTP tests:

Set the env vars and run the app locally via:

```bash
export CM_ATSPM_CLIENT_BASE_URL=...
export CM_ATSPM_CLIENT_USERNAME=...
export CM_ATSPM_CLIENT_PASSWORD=...
./mvnw spring-boot:run
```

or run it in Docker.

Use Intellij HTTP Client or the VSCode REST Client to run the tests.

Run the `token.http` test first to initialize the service with a token.