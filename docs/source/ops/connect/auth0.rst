.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _auth0:

Setting Up Auth0
================

.. note::

   This is an Early Access feature. Note that this feature does not currently
   work with Daml 2.0. These docs refer to and use Daml 1.18. The feature is
   under active development and it will soon be available for the 2.x major
   release series.

In this section, we will walk through a complete setup of an entire Daml
system using Auth0 as its authentication provider.

.. note::

   These instructions include detailed steps to be performed through the Auth0
   UI, which we do not control. They have been tested on 2021-11-02. It is
   possible Auth0 has updated their UI since then in ways that invalidate parts
   of the instructions here; if you notice any discrepancy, please report it on
   `the forum <https://discuss.daml.com>`_.

Authentication v. Authorization
-------------------------------

In a complete Daml system, the Daml components only concern themselves with
*authorization*: requests are accompanied by a (signed) token that *claims* a
number of rights (such as the right to *act as*  a given party). The Daml
system will check the signature of the token, but will not perform any further
verification of the claims themselves.

On the other side of the fence, the *authentication system* needs to verify a
client's identity and, based on the result, provide them with an appropriate
token. It also needs to record the mapping of client identity to Daml party (or
parties), such that the same external identity keeps mapping to the same
on-ledger party over time.

Note that we need bidirectional communication between the Daml driver and the
authentication system: the authentication system needs to contact the Daml
driver to allocate new parties when a new user logs in, and the Daml driver
needs to contact the authentication system to fetch the public key used to
verify token signatures.

In the context of this section, the authentication system is Auth0. For more
information on the Daml side, see the :doc:`/app-dev/authorization` page.

Prerequisites
-------------

In order to follow along this guide, you will need:

- An Auth0 tenant. See
  `Auth0 documentation <https://auth0.com/docs/get-started/create-tenants>`_ for
  how to create one if you don't have one already. You should get a free,
  dev-only one when you create an Auth0 account.
- A DNS (or IP address) that Auth0 can reach, and on which you (will) run a
  JSON API instance. This will be used to create parties. Auth0 uses a
  `known set of IP addresses <https://auth0.com/docs/security/data-security/allowlist>`_
  that depends on the location you chose for your tenant, so if your
  application is not meant to be public you can use network rules to only let
  requests from these IPs through.
- To know the ``ledgerId`` your ledger self-identifies as. Refer to your
  specific driver's documentation for how to set the ``ledgerId`` value.
- An application you want to deploy on your Daml system. This is not, strictly
  speaking, required, but the whole experience is going to be a lot less
  satisfying if you don't end up with something actually running on your Daml
  system. In this guide, we'll use the `create-daml-app` template,
  which supports Auth0 out-of-the-box on its UI side.

Generate Party Allocation Credentials
-------------------------------------

Since Auth0 will be in charge of requesting the allocation of parties, the
first logical step is to make it generate a token that can be used to allocate
parties. This may seem recursive at first, but the token used to allocate
parties only needs to have the ``admin`` field set to ``true``; it does not
require any preexisting party and does not need any ``actAs`` or ``readAs``
privileges.

In Auth0 concepts, we first need to register
`an API <https://auth0.com/docs/get-started/set-up-apis>`_. To do so, from the
`Auth0 Dashboard <https://manage.auth0.com/>`_, open up the Applications ->
APIs page from the menu on the left and click Create API in the top right.

You can choose any name for the API; for the purposes of this document, we'll
assume this API is named ``API_NAME``. The other parameters, however, are not
free to set: the API identifier **has to be** ``https://daml.com/ledger-api``,
and the signing algorithm **has to be** RS256 (which should be selected by
default). Creating the API should automatically create a Machine-to-Machine
application "API_NAME (Test Application)", which we will be using to generate
our tokens. You can change its name to a more appropriate one; for the
remainder of this document, we will assume it is called ADMIN_TOKEN_APP.

Navigate to that application's settings page (menu on the left: Applications >
Applications page, then click on the application's name). This is where you can
rename the application and find out about its Client ID and Client Secret,
which we'll need later on.

Now that we have an API and an application, we can generate a token with the
appropriate claims. In order to do that, we need to make an Auth0 Action.

In the menu on the left, navigate to Actions > Library, then click on Build
Custom in the top right. You can choose an appropriate name for your action;
we'll call it ADMIN_TOKEN_ACTION. Set the Trigger field to
"M2M/Client-Credentials", and leave the version of Node to the recommended one.
(These instructions have been tested with Node 16.)

This will open a text editor where you can add JavaScript code that will
trigger on M2M (machine to machine) connections. Replace the entire text box
content with:

.. code-block:: javascript

   exports.onExecuteCredentialsExchange = async (event, api) => {
     if (event.client.client_id === "%%ADMIN_TOKEN_ID%%") {
       api.accessToken.setCustomClaim(
         "https://daml.com/ledger-api",
         {
           "ledgerId": "%%LEDGER_ID%%",
           "participantId": null,
           "applicationId": "party-creation",
           "admin": true,
           "actAs": []
         }
       );
     }
   };

You need to replace ``%%ADMIN_TOKEN_ID%%`` with the Client ID of the
ADMIN_TOKEN_APP application, and ``%%LEDGER_ID%%`` with your actual
``ledgerId`` value. You can freely choose the ``applicationId`` value, and
should set an appropriate ``participantId`` if your Daml driver requires it.

You then need to click on Deploy in the top right to save this Action. Despite
the text on the button, this does not (yet) deploy it anywhere.

In order to actually deploy it, we need to make that Action part of a Flow. In
the menu on the left, navigate through Actions > Flows, then choose Machine to
Machine. Drag the "ADMIN_TOKEN_ACTION" (in the "Custom" tab) box on the right
in-between the "Start" and "Complete" black circles in the middle. Click Apply.
Now your Action is "deployed" and, should you modify it, clicking on the Deploy
button *would* directly affect your live setup.

At this point you should be able to verify, using the curl command from the
"Quick Start" tab of the M2M application, that you get a token. You should also
be able to check that the token has the expected claims. You can do that by
piping the result of the curl command through:

.. code-block:: bash

   cat curl-result.json | jq -r '.access_token' | sed 's/.*\.\(.*\)\..*/\1/' | base64 -d

JWKS Endpoint
-------------

In order to verify the tokens it receives, the Daml driver needs to know the
public key that matches the secret key used to sign them. Daml drivers use a
standard protocol for that called JWKS; in practice, this means giving the Daml
driver an HTTP URL it can query to get the keys. In the case of Auth0, that URL
is located at ``/.well-known/jwks.json`` on the tenant.

The full address is

.. code-block:: bash

   https://%%AUTH0_DOMAIN%%/.well-known/jwks.json

You can find the value for ``%%AUTH0_DOMAIN%%`` in the Domain field of the
settings page for the ADMIN_TOKEN_APP application (or any other application on
the same tenant).

Dynamic Party Allocation
------------------------

At this point, we can generate an admin token, and the Daml driver can check
its signature and thus accept it. The next step is to actually allocate
parties when people connect for the first time.

First, we need to create a new application, of type "Single Page Web
Applications". We'll be calling it LOGIN_APP. Open up the Settings tab and
scroll down to "Allowed Callback URLs". There, add your application's origin
(scheme, domain or IP, and port) to all three of Allowed Callback URLs, Allowed
Logout URLs and Allowed Web Origins. Scroll all the way down and click "Save
Changes".

Create a new Action (left menu > Actions > Library, top-right Build Custom
button). As usual, you can choose the name; we'll call it LOGIN_ACTION. Its
type should be "Login / Post Login".

Replace the default code with the following JavaScript:

.. code-block:: javascript

    const axios = require('axios');
    // only required if JSON API is behind self-signed cert
    // const https = require('https');

    exports.onExecutePostLogin = async (event, api) => {
      async function getParty() {
        if (event.user.app_metadata.party !== undefined) {
          return event.user.app_metadata.party;
        } else {
          const tokenResponse = await axios.request({
            "url": "%%ADMIN_TOKEN_URL%%",
            "method": "post",
            "data": {
              "client_id": "%%ADMIN_TOKEN_ID%%",
              "client_secret": "%%ADMIN_TOKEN_SECRET%%",
              "audience": "https://daml.com/ledger-api",
              "grant_type": "client_credentials"
            },
            "headers": {
              "Content-Type": "application/json",
              "Accept": "application/json"
            }
          });
          const token = tokenResponse.data.access_token;
          const partyResponse = await axios.request({
            "url": "%%ORIGIN%%/v1/parties/allocate",
            "method": "post",
            "headers": {
              "Content-Type": "application/json",
              "Accept": "application/json",
              "Authorization": "Bearer " + token
            },
            "data": {}
            // only required if JSON API is behind self-signed cert
            //, httpsAgent: new https.Agent({ rejectUnauthorized: false })
          });
          const party = partyResponse.data.result.identifier;
          api.user.setAppMetadata("party", party);

          // optional one-time setup like creating contracts etc. here

          return party;
        }
      };
      function setToken(party, actAs = [party], readAs = [party], applicationId = event.client.name) {
        api.idToken.setCustomClaim("https://daml.com/ledger-api", party);
        api.accessToken.setCustomClaim(
          "https://daml.com/ledger-api",
          {
            "ledgerId": "%%LEDGER_ID%%",
            "participantId": null,
            "applicationId": applicationId,
            "actAs": actAs,
            "readAs": readAs,
          });
      };
      if (event.client.client_id === "%%LOGIN_ID%%") {
        const party = await getParty();
        setToken(party);
      }
    };

where you need to replace ``%%LOGIN_ID%%`` with the Client ID of the LOGIN_APP
application; ``%%ADMIN_TOKEN_URL%%``, ``%%ADMIN_TOKEN_ID%%`` and
``%%ADMIN_TOKEN_SECRET%%`` with, respectively, the URL, ``client_id`` and
``client_secret`` values that you can find on the curl example from the Quick
Start of the ADMIN_TOKEN_APP application; ``%%ORIGIN%%`` by the domain
(or IP address) and port where Auth0 can reach your JSON API instance; and
``%%LEDGER_ID%%`` by the ``ledgerId`` you're passing into your Daml driver.

Before we can click on Deploy to save (but not deploy) this snippet, we need to
do one more thing. This snippet is using a library called ``axios`` to make
HTTP calls; we need to tell Auth0 about that, so it can provision the library
at runtime.

To do that, click on the little box icon to the left of code editor, then on
the button Add Module that just got revealed, and type in ``axios`` for the
name and ``0.21.1`` for the version. Then, click the Create button, and then
the Deploy button.

Now you need to go to Actions > Flows, choose the Login flow, and drag the
LOGIN_ACTION action in-between the two black circles Start and Complete.

Click Apply. You now have a working Auth0 system that automatically allocates
new parties upon first login, and remembers the mapping for future logins (that
happens by setting the party in the "app metadata", which Auth0 persists).

.. note::

   If you are hosting your JSON API instance behind a self-signed certificate
   (Auth0 absolutely requires TLS, but can be made to work with a self-signed
   cert), you'll need to uncomment the ``https`` import and the ``httpsAgent``
   line above. The ``https`` module does not require extra setup (unlike the
   ``axios`` one).

Token Refresh for Trigger Service
---------------------------------

If you want your users to be able to run triggers, you can run an instance of
the Trigger Service and expose it through the same HTTP URL. Because the
Trigger Service (via the Auth Middleware) will need "refreshable" tokens,
though, we need a bit of extra setup for that to work.

The first step on that front is to actually allow our tokens to be refreshed.
Go to the settings tab of the API_NAME API (menu on the left > Applications >
API > API_NAME) and scroll down. Towards the bottom of the page there should be
a "Allow Offline Access" toggle, which is off by default. Turn it on, and save.

Next, we need to create a second "Machine-to-Machine Application", which we'll
call OAUTH_APP, to register the OAuth2 Middleware which will refresh tokens for
the Trigger service. When creating such an application, you'll be asked for its
authorized APIs; select API_NAME. Once the application is created, go to its
settings tab and add ``%%ORIGIN%%/auth/cb`` as a callback URL.

You also need to scroll all the way down to the Advanced Settings section, open
the Grant Types tab, and enable "Authorization Code". Don't forget to save your
changes.

Finally, we need to extend our LOGIN_ACTION to respond to requests from the
OAuth2 Middleware. Navigate back to the Action code (left menu > Actions >
Library > Custom > click on LOGIN_ACTION) and add a second branch to the main
``if`` (new code starting on the line with ``CHANGES START HERE``; everything
before that should remain unchanged).

.. code-block:: javascript

    const axios = require('axios');
    // only required if JSON API is behind self-signed cert
    // const https = require('https');

    exports.onExecutePostLogin = async (event, api) => {
      async function getParty() {
        // unchanged
      };
      function setToken(party, actAs = [party], readAs = [party], applicationId = event.client.name) {
        // unchanged
      };
      if (event.client.client_id === "%%LOGIN_ID%%") {
        const party = await getParty();
        setToken(party);
        // CHANGES START HERE
      } else if (event.client.client_id === "%%OAUTH_ID%%") {
        const party = await getParty();
        const readAs = [];
        const actAs = [];
        let appId = undefined;
        event.transaction.requested_scopes.forEach(s => {
          if (s === "admin") {
            api.access.deny("Current user is not authorized for admin token.");
          } else if (s.startsWith("readAs:")) {
            const requested_read = s.slice(7);
            if (requested_read === party) {
              readAs.push(requested_read);
            } else {
              api.access.deny("Requested unauthorized readAs: " + requested_read);
            }
          } else if (s.startsWith("actAs:")) {
            const requested_act = s.slice(6);
            if (requested_act === party) {
              actAs.push(requested_act);
            } else {
              api.access.deny("Requested unauthorized actAs: " + requested_act)
            }
          } else if (s.startsWith("applicationId:")) {
            appId = s.slice(14);
          }
        });
        setToken(party, actAs, readAs, appId);
      }
    };

Where ``%%OAUTH_ID%%`` is the Client ID of the OAUTH_APP. The OAuth2 Middleware
will send a request with a number of *requested scopes*; the above code shows
how to walk through them as well as a simple approach to handling them. You can
change this code to fit your application's requirements.

Don't forget to click on Deploy to save your changes. This time, as the Action
is already part of a Flow, clicking the Deploy button really deploys the Action
and there is no further action needed.

Run Your App
------------

Prepare Your Application
************************

You may have an application already. In that case, use that. For the purposes
of illustration, here we're going to work with a modified version of
``create-daml-app``.

.. code-block:: bash

    daml new --template=gsg-trigger my-project

The next step is to build the Daml code:

.. code-block:: bash

    cd my-project
    daml build
    daml codegen js .daml/dist/my-project-0.1.0.dar -o ui/daml.js

Next, we'll build our frontend code, but first we're going to make a small
change to let us demonstrate interactions with the Trigger Service.

We'll need the package ID of the main DAR for the next step, so first collect
it by running:

.. code-block:: bash

    daml damlc inspect .daml/dist/my-project-0.1.0.dar | head -1

from the root of the project. In the following, we'll refer to it as
``%%PACKAGE_ID%%``.

Open up ``ui/src/components/MainView.tsx`` and add the ``Button`` component to
the existing imports from ``semantic-ui-react``:

.. code-block:: typescript

    import { Container, Grid, Header, Icon, Segment, Divider, Button } from 'semantic-ui-react';

Scroll down a little bit, and add the following code after the ``USERS_END``
tag (around line 18):

.. code-block:: typescript

    const trig = (url: string, req: object) => async () => {
      const resp = await fetch(url, req);
      if (resp.status === 401) {
        const challenge = await resp.json();
        console.log(`Unauthorized ${JSON.stringify(challenge)}`);
        var loginUrl = new URL(challenge.login);
        loginUrl.searchParams.append("redirect_uri", window.location.href);
        window.location.replace(loginUrl.href);
      } else {
        const body = await resp.text();
        console.log(`(${resp.status}) ${body}`);
      }
    }

    const list = trig("/trigger/v1/triggers?party=" + username, {});
    const start = trig("/trigger/v1/triggers", {
      method: "POST",
      body: JSON.stringify({
        triggerName: "%%PACKAGE_ID%%:ChatBot:autoReply",
        party: username,
        applicationId: "frontend"
      }),
      headers: {
        'Content-Type': 'application/json'
    }});

where ``%%PACKAGE_ID%%`` is the package ID of the main DAR file, as explained
above.

Finally, scroll down to the end of the ``Grid.Column`` tag, and add:

.. code-block:: tsx

    // ...
      </Segment>
      <Segment>
        <Button primary fluid onClick={list}>List triggers</Button>
        <Button primary fluid onClick={start}>Start autoReply</Button>
      </Segment>
    </Grid.Column>

Now, build your frontend with (starting at the root):

.. code-block:: bash

    cd ui
    npm install
    REACT_APP_AUTH=auth0 \
    REACT_APP_AUTH0_DOMAIN=%%AUTH0_DOMAIN%% \
    REACT_APP_AUTH0_CLIENT_ID=%%LOGIN_ID%% \
    npm run-script build

As before, ``%%AUTH0_DOMAIN%%`` and ``%%LOGIN_ID%%`` need to be replaced.

Now, we need to expose the JSON API and our static files. We'll use nginx
for that, but you can use any HTTP server you (and your security team) are
comfortable with, as long as it can serve static files and proxy some paths.

First, create a file ``nginx/nginx.conf.sh`` with the following content next to
your app folder, i.e. in our example ``nginx`` is a sibling to ``my-project``.

.. code-block:: bash

    #!/usr/bin/env bash

    set -euo pipefail
    openssl req -x509 \
                -newkey rsa:4096 \
                -keyout /etc/ssl/private/nginx-selfsigned.key \
                -out /etc/ssl/certs/nginx-selfsigned.crt \
                -days 365 \
                -nodes \
                -subj "/C=US/ST=Oregon/L=Portland/O=Company Name/OU=Org/CN=${FRONTEND_IP}"
    openssl dhparam -out /etc/ssl/certs/dhparam.pem 2048
    cat <<NGINX_CONFIG > /etc/nginx/nginx.conf
    worker_processes auto;
    pid /run/nginx.pid;
    events {
      worker_connections 768;
    }
    http {
      sendfile on;
      tcp_nopush on;
      tcp_nodelay on;
      keepalive_timeout 65;
      types_hash_max_size 2048;
      include /etc/nginx/mime.types;
      default_type application/octet-stream;
      access_log /var/log/nginx/access.log;
      error_log /var/log/nginx/error.log;
      gzip on;

      ssl_certificate /etc/ssl/certs/nginx-selfsigned.crt;
      ssl_certificate_key /etc/ssl/private/nginx-selfsigned.key;
      ssl_protocols TLSv1 TLSv1.1 TLSv1.2;
      ssl_prefer_server_ciphers on;
      ssl_ciphers "EECDH+AESGCM:EDH+AESGCM:AES256+EECDH:AES256+EDH";
      ssl_ecdh_curve secp384r1;
      ssl_session_cache shared:SSL:10m;
      ssl_session_tickets off;
      ssl_stapling on;
      ssl_stapling_verify on;
      resolver 8.8.8.8 8.8.4.4 valid=300s;
      resolver_timeout 5s;
      add_header X-Frame-Options DENY;
      add_header X-Content-Type-Options nosniff;

      ssl_dhparam /etc/ssl/certs/dhparam.pem;

      server {
        listen 80;
        return 302 https://${FRONTEND_IP}\$request_uri;
      }

      server {
        listen 443 ssl http2;
        location /v1/stream {
          proxy_pass http://${JSON_IP};
          proxy_http_version 1.1;
          proxy_set_header Upgrade \$http_upgrade;
          proxy_set_header Connection "Upgrade";
          proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        }
        location /v1 {
          proxy_pass http://${JSON_IP};
          proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        }
        location /auth/ {
          proxy_pass http://${AUTH_IP}/;
          proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        }
        location /trigger/ {
          proxy_pass http://${TRIGGER_IP}/;
          proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        }
        root /app/ui;
        index index.html;
        location / {
          # for development, uncomment proxy_pass and comment the try_files line
          #proxy_pass http://localhost:3000/;
          try_files \$uri \$uri/ =404;
        }
      }
    }
    NGINX_CONFIG

Next, create a file ``nginx/Dockerfile`` with this content:

.. code-block:: bash

    FROM nginx:1.21.0

    COPY build /app/ui
    COPY nginx.conf.sh /app/nginx.conf.sh
    RUN chmod +x /app/nginx.conf.sh
    CMD /app/nginx.conf.sh && exec nginx -g 'daemon off;'

Finally, we can build the Docker container with the following, starting
in the folder that contains both ``nginx`` and ``my-project``:

.. code-block:: bash

    cp -r my-project/ui/build nginx/build
    cd nginx
    docker build -t frontend .

And that's it for building the application. We now have a DAR file that is
ready to be deployed to a ledger, as well as a Docker container ready to serve
our frontend. All we need now is to get a Daml system up and running.
We document two paths forward here: one that relies on the Helm chart included
in Daml Enterprise, and a manual setup using only the Open Source SDK.

Use the Daml Helm Chart
***********************

For simplicity, we assume that you have access to a server with a public IP
address that both you and Auth0 can reach. Furthermore, we assume that you have
access to Daml Enterprise credentials to download the Docker images.  We
also assume you can create a local cluster with ``minikube`` on the remote
machine. Finally, we assume that you have downloaded the Helm chart in a folder
called ``daml-connect``.

First, start a new cluster::

  minikube start

Next, load up your credentials as explained in the :ref:`connect-helm-chart`
section. We assume they are loaded under the secret named
``daml-docker-credentials``.

Create a file called ``values.yaml`` with the following content:

.. code-block:: yaml

   imagePullSecret: daml-docker-credentials
   authUrl: "https://%%AUTH0_DOMAIN%%/.well-known/jwks.json"
   oauthMiddleware:
     create: true
     oauthAuth: "https://%%AUTH0_DOMAIN%%/authorize"
     oauthToken: "https://%%AUTH0_DOMAIN%%/oauth/token"
     callback: "https://%%DOMAIN%%/auth/cb"
     clientId: "%%OAUTH_ID%%"
     clientSecret: "%%OAUTH_SECRET%%"
   triggerService:
     create: true
     authExternal: "https://%%DOMAIN%%/auth"
     authCallback: "https://%%DOMAIN%%/trigger/cb"

where, as before:

- ``%%AUTH0_DOMAIN%%`` is the domain of your Auth0 tenant, displayed as the
  "Domain" property of any app within the tenant.
- ``%%DOMAIN%%`` is the domain on which your frontend will be exposed, and in
  particular here the domain to which Auth0 needs to redirect after the OAuth
  handshake.
- ``%%OAUTH_ID%%`` is, as before, the OAUTH_APP application's Client ID.
- ``%%OAUTH_SECRET`` is the same application's Client Secret.

Assuming that you have your Artifactory credentials in the environment
variables ``ARTIFACTORY_USERNAME`` (user name) and ``ARTIFACTORY_PASSWORD``
(API key), you can add the Helm repository with::

  helm repo add daml \
    https://digitalasset.jfrog.io/artifactory/connect-helm-chart \
    --username $ARTIFACTORY_USERNAME \
    --password $ARTIFACTORY_PASSWORD

And now, you can deploy your cluster::

  helm install dm daml/daml-connect --devel --values values.yaml

which will start the demo, non-production mode of the Helm chart. You can now
start your application with::

  PROXY="$(minikube ip):$(kubectl get svc dm-daml-connect-reverse-proxy --output=json | jq '.spec.ports[0].nodePort')"
  docker run -e JSON_IP=$PROXY \
             -e AUTH_IP=$PROXY/auth \
             -e TRIGGER_IP=$PROXY/trigger \
             -e FRONTEND_IP=$DOMAIN \
             --network=host \
             frontend

where ``$DOMAIN`` is assumed to be an environment variable set to the public
domain on which your server is exposed. And voilà! Your application is up and
running. You should be able to log in with Auth0, exchange messages, and set up
an auto-reply trigger, all by connecting your browser to ``https://$DOMAIN/``.

Set Up the Daml Components Manually 
***********************************

For simplicity, we assume that all of the Daml components will run on a single
machine (they can find each other on ``localhost``) and that this machine has
either a public IP or a public DNS that Auth0 can reach (hereafter assumed to
be set as the ``DOMAIN`` env var). Furthermore, we assume that IP/DNS is what
you've configured as the callback URL in the Auth0 configuration above.

Finally, we assume that you can SSH into that machine and run ``daml`` and
``docker`` commands on it.

The rest of this section happens on that remote server.

First, we need to start the Daml driver. For this example we'll use the
sandbox, but with ``--implicit-party-allocation false`` it should behave like a
production ledger (minus persistence).

.. code-block:: bash

    daml sandbox --ledgerid %%LEDGER_ID%% \
                 --auth-jwt-rs256-jwks https://%%AUTH0_DOMAIN%%/.well-known/jwks.json \
                 --implicit-party-allocation false \
                 --dar .daml/dist/my-project-0.1.0.dar

As before, you need to replace ``%%LEDGER_ID%%`` with a value of your choosing
(the same one you used when configuring Auth0), and ``%%AUTH0_DOMAIN%%`` with
your Auth0 domain, which you can find as the Domain field at the top of the
Settings tab for any app in the tenant.

Next, you need to start a JSON API instance.

.. code-block:: bash

    cd my-project
    daml json-api --ledger-port 6865 \
                  --ledger-host localhost \
                  --http-port 4000

Then, we want to start the Trigger Service and OAuth2 middleware, which we will
put respectively under ``/trigger`` and ``/auth``. First, the middleware:

.. code-block:: bash

    DAML_CLIENT_ID=%%OAUTH_APP_ID%% \
    DAML_CLIENT_SECRET=%%OAUTH_APP_SECRET%% \
    daml oauth2-middleware \
      --address localhost \
      --http-port 5000 \
      --oauth-auth "https://%%AUTH0_DOMAIN%%/authorize" \
      --oauth-token "https://%%AUTH0_DOMAIN%%/oauth/token" \
      --auth-jwt-rs256-jwks "https://%%AUTH0_DOMAIN%%/.well-known/jwks.json" \
      --callback %%ORIGIN%%/auth/cb

where, as before, you need to replace:

- ``%%OAUTH_APP_ID%%`` with the Client ID value you can find at the top of the
  settings tab for the OAUTH_APP we just created.
- ``%%OAUTH_APP_SECRET%%`` with the Client Secret value you can find at the top
  of the settings tab for the OAUTH_APP we just created.
- ``%%AUTH0_DOMAIN%%`` with your tenant domain.
- ``%%ORIGIN%%`` with the full domain-name-or-ip & port, including scheme,
  under which you expose your server.

Now, the trigger service:

.. code-block:: bash

    daml trigger-service \
      --address localhost \
      --http-port 6000 \
      --ledger-host localhost \
      --ledger-port 6865 \
      --auth-internal http://localhost:5000 \
      --auth-external %%ORIGIN%%/auth \
      --auth-callback %%ORIGIN%%/trigger/cb \
      --dar .daml/dist/my-project-0.1.0.dar

where ``%%ORIGIN%%`` is, as per the Auth0 configuration, ``https://$DOMAIN``.

And that's all the Daml components. You can now start your frontend application
with::

    docker run -e JSON_IP=localhost:4000 \
               -e AUTH_IP=localhost:5000 \
               -e TRIGGER_IP=localhost:6000 \
               -e FRONTEND_IP=$DOMAIN \
               --network=host frontend

This runs a "production build" of your frontend code. If instead you want to
develop frontend code against the rest of this setup, you can uncomment the
last ``proxy_pass`` directive in ``nginx.conf.sh``, comment the ``try_files``
line after it, and start a reloading development server with:

.. code-block:: bash

    cd ui
    npm install
    REACT_APP_AUTH=auth0 \
    REACT_APP_AUTH0_DOMAIN=%%AUTH0_DOMAIN%% \
    REACT_APP_AUTH0_CLIENT_ID=%%LOGIN_ID%% \
    npm start
