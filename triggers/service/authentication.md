# Trigger Service Authentication

The trigger service is a client of the
[reference authentication service](https://github.com/digital-asset/ref-ledger-authenticator),
which we'll just call the "auth service".
It is used to obtain tokens used to access the ledger on behalf of trigger service users.

## Auth service concepts

The auth service has the following important notions:
 * user
 * service account (SA)
 * SA credential
 * SA token

In general, each item in the list can have a one-to-many relationship with an item later in the list.
For our purposes, users and service accounts correspond one-to-one and represent parties using the trigger service.
A service account is used to obtain an SA credential, which in turn is used to obtain an SA token which is used to authenticate ledger requests.

It is important to note the validity windows for each of these items as they influence our authentication flow.
Users and service accounts are valid forever.
However, both SA credentials and SA tokens have validity windows: 30 days for an SA credential and 1 day for an SA token.
For this reason there may be many SA credentials per service account, and many SA tokens per SA credential.
Note that there is also a bearer token used to interact with the auth service with a validity window of 1 day.

The auth service endpoints that are relevant to us are:
 * `authorize`: uses basic auth with a username/password pair to register a user and returns a JWT token to be used as an OAuth2 bearer token for other endpoints besides `login` (valid for 1 day)
 * `request`: asynchronously creates a service account for the user (identified by the bearer token in the request header) to access a ledger with the given ledger id
 * “list” (GET on `sa/secure/`): returns the details for all service accounts associated with the user (in our case there should be at most one), including the service account id and allocated credential ids for each
 * `credRequest`: asynchronously creates an SA credential request for the given user and service account id (to be consumed by an auth service bot)
 * `cred`: retrieves the SA credential associated with the given credential id (valid 30 days)
 * `login`: uses (credential id, credential string) pair for basic auth and returns an SA token for ledger access (valid 1 day)

## Authentication flow

For a new party using the trigger service, we must:
 1. `authorize` with basic username/password credentials to get an auth service bearer token
 1. `request` a service account using the above token as an OAuth2 bearer token
 1. send “list” requests in a polling fashion (the service account is created asynchronously) until a service account appears in the response (this is a ServiceAccountResponse with a service account id but no credential ids)
 1. `credRequest` with the party’s service account id 
 1. poll “list” request until the ServiceAccountResponse has a new credential id at the front of the creds list
 1. `cred` request with the new credential id to get a ServiceAccountCredentialResponse with the SA credential details
 1. `login` using the (credential id, credential string) pair for basic auth to receive an SA token

This token is used in the ledger client config and is sent with ledger requests for that party.
When the SA token expires after 1 day, we repeat the `login` step (7) using the same credential (as it is valid for 30 days).
Finally when the credential expires, we repeat step 1 (as our bearer token for that party will also have expired) and then steps 4-7.

In the context of the trigger service, we detect that an SA token has expired by a trigger failing with an "unauthorized" error.
Our trigger retry logic will need to request a new SA token.
We can directly check that the credential is still valid before requesting a new SA token with `login`.

For a party using the trigger service after the first time, we will look up their service account and see if there is a valid SA credential associated with it.
Depending on that, we enter the flow at step 4 (requesting a new SA credential) or step 7 (`login` with an existing credential).

## Example flow using the command line

In one terminal window, run a sandbox which the auth service will use as its "admin ledger".
(It is an implementation choice of the auth service to use a DAML ledger for managing users, service accounts, credentials, etc).
```
❯ daml sandbox
```

In another terminal window, build and run the auth service.
We run with the "test mode" which allows us to authorize users using basic username/password credentials.
```
❯ make clean assembly
❯ DABL_AUTHENTICATION_SERVICE_TEST_MODE=true java -jar target/scala-2.12/authentication-service-assembly-0.1.jar
```

In a third window, we'll execute requests to the auth service using `curl`.

```
❯ curl -X POST localhost:8089/sa/secure/authorize -u user:pass
{"token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImEtcy1lMDZmZDVjMy00NWM1LTRjZGQtYTI1OC1iMmIzMmIyNjA3NDYifQ.eyJpc3MiOiJsb2NhbGhvc3QvbG9naW4iLCJzdWIiOiJ1c2VyIiwiZXhwIjoxNTk0ODI3NTIwLCJsZWRnZXJJZCI6Ii5hdXRoIiwicGFydHkiOiJ1c2VyIiwicmlnaHRzIjpbInJlYWQiLCJ3cml0ZTpjcmVhdGUiLCJ3cml0ZTpleGVyY2lzZSJdfQ.NiOHuA3Zm6yAs2ThSlXzlGyffbjauJLBjlnD8k8Qi-BBsya45bD0k4cLr-NMcUeabyoEHWVdDuPm7CoUCRkMqePBU5Q9FcwNyU3pqUdiRwmsLLEnFfaSi5mXV9JDH8qlw59LBLXOu5_D2i3p5mC4WB6hvLjqocJG7hguG5pmYZ4"}
```

```
❯ curl -X GET localhost:8089/sa/secure/me --oauth2-bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImEtcy1lMDZmZDVjMy00NWM1LTRjZGQtYTI1OC1iMmIzMmIyNjA3NDYifQ.eyJpc3MiOiJsb2NhbGhvc3QvbG9naW4iLCJzdWIiOiJ1c2VyIiwiZXhwIjoxNTk0ODI3NTIwLCJsZWRnZXJJZCI6Ii5hdXRoIiwicGFydHkiOiJ1c2VyIiwicmlnaHRzIjpbInJlYWQiLCJ3cml0ZTpjcmVhdGUiLCJ3cml0ZTpleGVyY2lzZSJdfQ.NiOHuA3Zm6yAs2ThSlXzlGyffbjauJLBjlnD8k8Qi-BBsya45bD0k4cLr-NMcUeabyoEHWVdDuPm7CoUCRkMqePBU5Q9FcwNyU3pqUdiRwmsLLEnFfaSi5mXV9JDH8qlw59LBLXOu5_D2i3p5mC4WB6hvLjqocJG7hguG5pmYZ4
{
  "user": "sa-user-21a4c943-57cc-46e7-b07e-380d2c780622"
}
```

We'll write `--oauth2-bearer ...` in the following requests to refer to same bearer token as used above.
Also note that the service account `request` below refers to a `test-ledger` id of the ledger we are
granting authentication to (not the admin ledger).
```
❯ curl -X POST localhost:8089/sa/secure/request/test-ledger -H "Content-type: application/json" -d '{"nonce": "random-nonce"}' --oauth2-bearer ...

❯ curl -X GET localhost:8089/sa/secure --oauth2-bearer ...
{"serviceAccounts":[{"creds":[],"nonce":"random-nonce","serviceAccount":"sa-d007e6a6-7af6-41de-890d-96647c4b23f8"}]}

❯ curl -X POST localhost:8089/sa/secure/sa-d007e6a6-7af6-41de-890d-96647c4b23f8/credRequest --oauth2-bearer ...

❯ curl -X GET localhost:8089/sa/secure --oauth2-bearer ...
{"serviceAccounts":[{"creds":[{"credId":"cred-80f27291-7fce-4b0d-b77f-cd3671edf474"}],"nonce":"random-nonce","serviceAccount":"sa-d007e6a6-7af6-41de-890d-96647c4b23f8"}]}

❯ curl -X GET localhost:8089/sa/secure/cred/cred-80f27291-7fce-4b0d-b77f-cd3671edf474 --oauth2-bearer ...
{
  "cred": "O0hR6WrOawvf1tPJ",
  "credId": "cred-80f27291-7fce-4b0d-b77f-cd3671edf474",
  "nonce": "random-nonce",
  "validFrom": "2020-07-14T15:50:01.378721Z",
  "validTo": "2020-08-13T15:50:01.378721Z"
}

❯ curl -X POST localhost:8089/sa/login -u cred-80f27291-7fce-4b0d-b77f-cd3671edf474:O0hR6WrOawvf1tPJ
{"token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImEtcy1lMDZmZDVjMy00NWM1LTRjZGQtYTI1OC1iMmIzMmIyNjA3NDYifQ.eyJpc3MiOiJsb2NhbGhvc3QvbG9naW4iLCJzdWIiOiJzYS1kMDA3ZTZhNi03YWY2LTQxZGUtODkwZC05NjY0N2M0YjIzZjgiLCJleHAiOjE1OTQ4Mjg5MDYsImxlZGdlcklkIjoidGVzdC1sZWRnZXIiLCJwYXJ0eSI6InNhLWQwMDdlNmE2LTdhZjYtNDFkZS04OTBkLTk2NjQ3YzRiMjNmOCIsInJpZ2h0cyI6WyJyZWFkIiwid3JpdGU6Y3JlYXRlIiwid3JpdGU6ZXhlcmNpc2UiXX0.JczDfJDEQzdqHEterHCXF-_PejUkFkeeE7SnTfBDqIVuhTIk9Bzh-rBUuMZ-ae0jxMyIB5rU9qlbn64vkqOD6xqqiR1IlIg1mxoaqKaomvy0ernx8mD_SqcDDKXCIMKaA8VEZOXVjvvZQGjVCzRK15Gy902ki-CXryKYjXxbYBQ"}
```

This final token is the one used to authenticate requests to the `test-ledger`.