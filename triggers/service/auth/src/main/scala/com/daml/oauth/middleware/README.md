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
* Create a new native application.
  - Provide a name (`ex-daml-auth-middleware`).
  - Select the authorized API (`ex-daml-api`).
  - Configure the allowed callback URLs in the settings (`http://localhost:3000`).
  - Note the "Client ID" and "Client Secret" displayed in the "Basic
    Information" pane of the application settings.
  - Note the "OAuth Authorization URL" and the "OAuth Token URL" in the
    "Endpoints" tab of the advanced settings.
* Create a "Hook" for "Client Credential Exchange".
  - Provide a name (`ex-daml-claims`).
  - Provide a script
    ``` javascript
    /**
    @param {object} client - information about the client
    @param {string} client.name - name of client
    @param {string} client.id - client id
    @param {string} client.tenant - Auth0 tenant name
    @param {object} client.metadata - client metadata
    @param {array|undefined} scope - array of strings representing the scope claim or undefined
    @param {string} audience - token's audience claim
    @param {object} context - additional authorization context
    @param {object} context.webtask - webtask context
    @param {function} cb - function (error, accessTokenClaims)
    */
    module.exports = function(client, scope, audience, context, cb) {
      var access_token = {};
      var actAs = [];
      var readAs = [];
      var admin = false;
      // TODO define mapping from scope to ledger claims.
      scope.forEach(s => {
        if (s.startsWith("actAs:")) {
          actAs.push(s.slice(6));
        } else if (s.startsWith("readAs:")) {
          readAs.push(s.slice(7));
        } else if (s.startsWith("admin")) {
          admin = true;
        }
      })
      access_token['https://daml.com/ledger-api'] = {
        // NOTE change the ledger ID to match your deployment.
        "ledgerId": "2D105384-CE61-4CCC-8E0E-37248BA935A3",
        "applicationId": client.name,
        "actAs": actAs,
        "readAs": readAs,
        "admin": admin
      };
      cb(null, access_token);
    };
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
