# Trigger Service Authentication

The trigger service is a client of the
[reference authentication service](https://github.com/digital-asset/ref-ledger-authenticator),
which we'll just call the "auth service".
It is used to obtain ledger access tokens on behalf of trigger service users.

The auth service has notions of users, service accounts and credentials.
In the general case, the relationship between the three are one to many to many.
For our purposes, users and service accounts are in one-to-one correspondence with parties who are clients of the trigger service.
Credentials correspond one-to-one with ledger access tokens.
A user will likely require several credentials as they expire over time.

The auth service endpoints that are relevant to us are:
* `authorize`: uses basic auth with a username/password pair to register a user and returns a JWT token to be used as an OAuth2 bearer token for other endpoints (besides `login`)
* `request`: asynchronously creates a service account for the user (identified in the bearer token in the request header) to access a ledger with the given ledger id
* “list” (GET on `sa/secure/`): returns the details for all service accounts associated with the user (in our case there should be at most one), including service account id and list of credential ids for each
* `credRequest`: asynchronously creates a credential request for the given user and service account id
* `cred`: retrieves the credential associated with the given credential id
* `login`: uses (credential id, credential string) pair for basic auth and returns a JWT token for ledger access

For a new party using the trigger service, we must:
* `authorize` with basic username/password credentials (creating a user contract on the auth service’s “admin” ledger) to get an auth service bearer token
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
