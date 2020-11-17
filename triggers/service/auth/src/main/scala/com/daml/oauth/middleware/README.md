# Trigger Service Authentication/Authorization Middleware

Implements an OAuth2 middleware according to the trigger service
authentication/authorization specification in
`triggers/service/authentication.md`.

## Manual Testing against Auth0

Apart from the automated tests defined in this repository, the middleware can
be tested manually against an auth0 OAuth2 setup. The necessary steps are
extracted and adapted from the [Secure DAML Infrastructure
repository](https://github.com/digital-asset/ex-secure-daml-infra).

### Setup

* Sign up for an account on [Auth0](https://auth0.com).
* Create a new API.
  - Provide a name (`ex-daml-api`).
  - Provide an Identifier (`https://daml.com/ledger-api`).
  - Select Signing Algorithm of `RS256`.
  - Allow offline access to enable refresh token generation.
    This allows the OAuth2 client, i.e. the auth middleware, to request access through a refresh token when the resource owner, i.e. the user, is offline. 
* Create a new native application.
  - Provide a name (`ex-daml-auth-middleware`).
  - Select the authorized API (`ex-daml-api`).
  - Configure the allowed callback URLs in the settings (`http://localhost:3000/cb`).
  - Note the "Client ID" and "Client Secret" displayed in the "Basic
    Information" pane of the application settings.
  - Note the "OAuth Authorization URL" and the "OAuth Token URL" in the
    "Endpoints" tab of the advanced settings.
* Create a new empty rule.
  - Provide a name (`ex-daml-claims`).
  - Provide a script
    ``` javascript
    function (user, context, callback) {
      // Only handle ledger-api audience.
      const audience = context.request.query && context.request.query.audience || "";
      if (audience !== "https://daml.com/ledger-api") {
        return callback(null, user, context);
      }

      // Grant all requested claims
      const scope = (context.request.query && context.request.query.scope || "").split(" ");
      var actAs = [];
      var readAs = [];
      var admin = false;
      scope.forEach(s => {
        if (s.startsWith("actAs:")) {
          actAs.push(s.slice(6));
        } else if (s.startsWith("readAs:")) {
          readAs.push(s.slice(7));
        } else if (s === "admin") {
          admin = true;
        }
      });

      // Construct access token.
      const namespace = 'https://daml.com/ledger-api';
      context.accessToken[namespace] = {
        // NOTE change the ledger ID to match your deployment.
        "ledgerId": "2D105384-CE61-4CCC-8E0E-37248BA935A3",
        "actAs": actAs,
        "readAs": readAs,
        "admin": admin
      };

      return callback(null, user, context);
    }
    ```
* Create a new user.
  - Provide an email address (`alice@localhost`)
  - Provide a secure password
  - Mark the email address as verified on the user's "Details" page.

### Testing

* Start the middleware by executing the following command.
  ```
  $ DAML_CLIENT_ID=CLIENTID \
    DAML_CLIENT_SECRET=CLIENTSECRET \
    bazel run //triggers/service/auth:oauth-middleware-binary -- \
      --port 3000 \
      --oauth-auth AUTHURL \
      --oauth-token TOKENURL
  ```
  - Replace `CLIENTID` and `CLIENTSECRET` by the "Client ID" and "Client
    Secret" from above.
  - Replace `AUTHURL` and `TOKENURL` by the "OAuth Authorization URL"
    and "OAuth Token URL" from above. They should look as follows:
    ```
    https://XYZ.auth0.com/authorize
    https://XYZ.auth0.com/oauth/token
    ```
- Browse to the middleware's login endpoint.
  - URL `http://localhost:3000/login?redirect_uri=callback&claims=actAs:Alice`
  - Login as the new user created above.
  - Authorize the middleware application to access the tenant.
  - You should be redirected to `http://localhost:3000/callback`.
