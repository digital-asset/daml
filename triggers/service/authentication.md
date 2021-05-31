# Design for Trigger Service Authentication/Authorization

## Goals

- Be compatible with an OAuth2 authorization code grant
  https://tools.ietf.org/html/rfc6749#section-4.1
- Do not require OAuth2 or any other specific
  authentication/authorization protocol from the IAM. In other words,
  the communication with the IAM must be pluggable.
- Do not rely on wildcard access for the trigger service, it should
  only be able to start triggers on behalf of a party if a user that
  controls that party has given consent.
- Support long-running triggers without constant user
  interaction. Since auth tokens are often short-lived (e.g., expire
  after 1h), this implies some mechanism for token refresh.

## Design

This involves 3 components:

1. The trigger service provided by DA.
2. An auth middleware. DA provides an implementation of this for at
   least the OAuth2 authorization code grant but this is completely
   pluggable so if the DA-provided middleware does not cover the IAM
   infrastructure of a client, they can implement their own.
3. The IAM. This is the entity that signs Ledger API tokens. This is
   not provided by DA. The Ledger is configured to trust this entity.

### Auth Middleware API

The auth middleware provides a few endpoints (the names don’t matter
all that much, they just need to be fixed once).

1. /auth The trigger service, will contact this endpoint with a set of
   claims passing along all cookies in the original request. If
   the user has already authenticated and is authorized for those
   claims, it will return an access token (an opaque blob to the
   trigger service) for at least those claims and a refresh token
   (another opaque blob). If not, it will return an unauthorized
   status code.

2. /login If /auth returned unauthorized, the trigger service will
   redirect users to this.
   For HTML requests via HTTP redirect, otherwise via a custom WWW-Authenticate challenge in a 401 resonse.
   The parameters will include the requested claims as well as an optional callback URL (note that this is not the OAuth2 callback url but a callback URL on the trigger service). This will start an auth flow,
   e.g., an OAuth2 authorization code grant. If the flow succeeds the
   auth service will set a cookie with the access and refresh token
   and redirect to the callback URL if present or return status code 200.
   At this point, a request to
   /auth will succeed (based on the cookie). If the flow failed the
   auth service will not set a cookie and redirect to the callback URL
   with an additional error and optional error_description parameter
   or return 403 with error and optional error_description in the response body.

3. /refresh This accepts a refresh token and returns a new access
   token and optionally a new refresh token (or fails).

### Auth Middleware Implementation based on OAuth2 Authorization Code Grant

1. /auth checks for the presence of a cookie with the tokens in it.
2. /login starts an OAuth2 authorization code grant flow. After the
   redirect URI is called by the authorization server, the middleware
   makes a request to get the tokens, sets them in cookies and
   redirects back to the callback URI. Upon failure the middleware
   forwards the error and error_description to the callback URI.
3. /refresh simply proxies to the refresh endpoint on the
   authorization server adding the client id and secret.

Note that the auth middleware does not need to persist any state in
this model. The trigger service does need to persist at least the
refresh token and potentially the access token.

## Related Projects

The design here is very close to existing OAuth2 middlewares/proxies
such as [Vouch](https://github.com/vouch/vouch-proxy) or [OAuth2
Proxy](https://github.com/oauth2-proxy/oauth2-proxy). There are two
main differences:

1. The trigger service extracts the required claims from the original
   request. This means that the first request goes to the trigger
   service and not to the proxy/middleware. The middleware shouldn’t
   have to know how to map a request to the set of required claims so
   this seems the only workable option.

2. The /auth request takes a custom set of claims. The existing
   proxies focus on OIDC and don’t support any custom claims.

Nevertheless, the fact that the design is very close to existing
proxies seems like a good thing.
