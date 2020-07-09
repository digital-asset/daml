# Trigger Service Authentication

The trigger service is a client of the
[reference authentication service](https://github.com/digital-asset/ref-ledger-authenticator),
which we'll just call the "auth service".
It is used to obtain tokens used to access the ledger on behalf of trigger service users.

## Auth service concepts

The auth service has the following important entities:
 * user
 * service account (SA)
 * SA credential
 * SA token

The SA token is a JWT token eventually used for ledger access.

In general, each entity in the list can have a one-to-many relationship with an entity later in the list.
For our purposes, users and service accounts correspond one-to-one and represent parties using the trigger service.
Service accounts do not expire.

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
 * `authorize` with basic username/password credentials to get an auth service bearer token
 * `request` a service account using the above token as an OAuth2 bearer token
 * send “list” requests in a polling fashion (the service account is created asynchronously) until a service account appears in the response (this is a ServiceAccountResponse with a service account id but no credential ids)

A new ledger access token is required for a party if either
 * it is the party’s first request to the trigger service, or
 * the party had a ledger access token which expired.

In these cases, we have the following flow to get a new token.
 * `credRequest` with the party’s service account id 
 * poll “list” request until the ServiceAccountResponse has a new credential id at the front of the creds list
 * `cred` request with the new credential id to get a ServiceAccountCredentialResponse with the full credential details
 * `login` using the (credential id, credential string) pair for basic auth to receive a new JWT ledger access token

This token is used in the ledger client config and is sent with ledger requests for that party, until it expires.
