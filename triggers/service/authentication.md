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

