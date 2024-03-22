.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _connect-helm-chart:

Daml Helm Chart
===============

.. note::

   This is an Early Access feature. Note that this feature does not currently
   work with Daml 2.0. These docs refer to and use Daml 1.18. The feature is
   under active development and it will soon be available for the 2.x major
   release series.

We provide an Early Access version of the Helm Chart for
Daml Enterprise customers. This page contains documentation for that Helm
chart.

Credentials
-----------

Like all Daml Enterprise components, the Helm Chart is hosted on
Artifactory. To get both the Helm chart itself and the Docker images it relies
on, you will need Artifactory credentials. In the rest of this document, we
assume that ``$ARTIFACTORY_USERNAME`` refers to your Artifactory user name,
whereas ``$ARTIFACTORY_PASSWORD`` refers to your Artifactory API key.

Install the Helm Chart Repository
---------------------------------

To let your local Helm installation know about the Daml Helm chart, you
need to add the repository with::

  helm repo add daml \
    https://digitalasset.jfrog.io/artifactory/connect-helm-chart \
    --username $ARTIFACTORY_USERNAME \
    --password $ARTIFACTORY_PASSWORD

This will install the repository as ``daml``; you can then list the available
versions with::

  helm search repo --devel -l daml

The ``--devel`` flag lets Helm know that you want to list prerelease versions
(in the Semver sense). To avoid any confusion as to the production-readiness of
the Helm chart, while the feature is in Early Access, only prerelease versions
of the Helm chart will be available.

Later on, you can update your local listing to match the Artifactory state
with::

  helm repo update

And you can deploy the latest prerelease version with::

  helm install dm daml/daml-connect --devel --values values.yaml

where ``values.yaml`` is a YAML file that includes at least the
``imagePullSecret`` key. See the rest of this page for other options in
``values.yaml``, and the Helm documentation for related Helm usage.

Set Up the ``imagePullSecret``
------------------------------

The Helm chart relies on the production-ready Docker images for individual
components that are part of Daml Enterprise. Specifically, it expects a
Kubernetes secret given as the ``imagePullSecret`` argument with the relevant
Docker credentials in it.

Here is an example script that would load said credentials in a secret named
``daml-docker-credentials``:

.. code-block:: bash

    #!/usr/bin/env bash

    set -euo pipefail

    if [ -z ${ARTIFACTORY_PASSWORD+x} ] || [ -z ${ARTIFACTORY_USERNAME+x} ]; then
        echo "Please input information from:"
        echo "https://digitalasset.jfrog.io/ui/admin/artifactory/user_profile"
        read -p "User Profile (first.last): " USERNAME
        read -p "API Key: " -s PASSWORD
    else
        USERNAME="$ARTIFACTORY_USERNAME"
        PASSWORD="$ARTIFACTORY_PASSWORD"
    fi

    temp=$(mktemp)
    trap "rm -f $temp" EXIT

    cred=$(echo -n "$USERNAME:$PASSWORD" | base64)
    jq -n --arg cred "$cred" '
      ["-daml-on-sql","-http-json","-oauth2-middleware","-trigger-service",""]
      | map({("digitalasset" + . + "-docker.jfrog.io"):{"auth":$cred}})
      | add
      | {auths: .}
      ' > $temp

    kubectl create secret generic daml-docker-credentials \
            --from-file=.dockerconfigjson=$temp \
            --type=kubernetes.io/dockerconfigjson

    rm -f $temp
    trap - EXIT

Running this script with the environment variables ``ARTIFACTORY_USERNAME`` and
``ARTIFACTORY_PASSWORD`` set will result in a non-interactive deployment of the
secret, which may be useful for CI environments.

Quickstart
----------

The Helm chart is designed to let you get started quickly, using a default
configuration that is decidedly **NOT MEANT FOR PRODUCTION USE**.

To get started against a development cluster, you can just run::

  helm install dm daml/daml-connect \
       --devel \
       --set imagePullSecret=daml-docker-credentials

This assumes you have used the above script to setup your credentials, or
otherwise created the secret ``daml-docker-credentials``. It also assumes you
run this command after having added the Daml Helm chart repository as
explained above.

This is going to start the following:

- For each of the state-keeping components (Daml driver for PostgreSQL, HTTP
  JSON API Service), an "internal" PostgreSQL database server. These are
  decidedly not production-ready. For a production setup, you'll need to
  provide your own databases here.
- A fake, testing-only JWT minter to serve as the authentication server. This
  should be replaced with a real authentication server for production use. See
  the :ref:`auth0` section for an example of using an external authentication
  infrastructure.
- A single instance of each of the following services: Daml driver for
  PostgreSQL, HTTP JSON API Service.
- An nginx server exposing the ``/v1`` endpoints of the HTTP JSON API Service
  on a ``NodePort`` service type, for easy access from outside the Kubernetes
  cluster.

If you set up the Trigger Service and/or the OAuth2 Middleware (without setting
the ``production`` flag), the reverse proxy will automatically proxy them too,
and a separate PostgreSQL instance will be started for the Trigger Service. See
the end of this page for details.

Production Setup
----------------

There are many options you may want to set for a production setup. See the
reference at the end of this page for full details. At a minimum, though, you
need to set the following:

- ``production=true``: By default, the Helm chart starts a number of components
  that are meant to give you a quick idea of what the Helm chart enables, but
  are most definitely not meant for production use. Specifically, this will
  disable the internal PostgreSQL instances, the mock auth server, and the
  reverse proxy.
- ``ledger.db``: If you want the Helm char to start a Daml driver For
  PostgreSQL instance for you, you need to set this. See reference section at
  the end of this page for details.
- ``ledger.host`` and ``ledger.port``: If you **do not** want the Helm chart to
  setup a Daml driver isntance for you, but instead want the components started
  by it to connect to an existing Ledger API server, fill in these options
  instead of the ``ledger.db`` object.
- ``jsonApi.db``: If you want the Helm chart to start the HTTP JSON API Service
  for you, you need to set this. See reference section at the end of this page
  for details.
- ``triggerService.db``: If you want the Helm chart to start the Trigger
  Service for you, you need to set this. See reference section at the end of
  this page for details.
- ``authUrl``: If you want the Helm chart to provide either a Daml driver for
  PostgreSQL or a OAuth2 Middleware instance, you will need to set this to the
  JWKS URL of your token provider.

If you start the Trigger Service, you will need to configure it, as well as the
OAuth2 Middleware. See the required options for them in the reference section
at the end of this page.

Finally, we also recommend looking at the ``resources`` option for each
component and adjusting them to fit your particular use-case.

Log Aggregation
---------------

All processes write their logs directly to stdout. This means that log
aggregation can be addressed at the Kubernetes level and does not require any
specific support from the Helm chart itself. One fairly easy way to achieve
this is using `Filebeat <https://www.elastic.co/beats/filebeat>`_, which
regulary collects the logs of your containers and ingests them into
`Elasticsearch <https://www.elastic.co/elasticsearch/>`,
`Logstash <https://www.elastic.co/logstash/>`_,
`Kafka <https://kafka.apache.org/>`_, etc.

You can find external documentation on, how to setup `ElasticSearch` with
`Filebeat` and `Kibana` for analyzing logs on your Kubernetes cluster
`here <https://www.deepnetwork.com/blog/2020/01/27/ELK-stack-filebeat-k8s-deployment.html>`_.

The :ref:`HTTP JSON API <json-api>` component in the Helm chart produces
JSON-encoded logs. Other components log as unstructured text.

Daml Metrics Options
--------------------

The Daml driver for PostgreSQL instance and the HTTP JSON API instances started
by the Helm chart are configured to expose Prometheus metrics on a port named
``metrics``, using the appropriate annotations. This means that, if you are
running a cluster-wide Prometheus instance, the relevant metrics should be
collected automatically.

See each component's documentation for details on the metrics exposed:

- `Daml driver for PostgreSQL </daml-driver-for-postgresql/#types-of-metrics>`_
- :ref:`JSON API metrics <json-api-metrics>`

Upgrade
-------

.. note::

   This section only makes sense with the ``production`` flag set to ``true``.

Upgrading the Daml version should be done by uninstalling the existing
Helm chart, waiting for all of the pods to stop, and then installing a higher
version. Destroying all of the components is a safe operation because all of
the state is stored in the provided database coordinates. There is no
additional state within the components themselves.

The components are not designed for running concurrently with older versions,
so it is imperative to wait for the existing Helm chart components to be
completely shut down before installing the new one. Do not try to upgrade in
place.

Assuming you do not change the database coordinates, you should have data
continuity through the upgrade.

Back Up
-------

.. note::

   This section only makes sense with the ``production`` flag set to ``true``.

For a production setup, you should be providing the Helm chart with external
database coordinates. The simplest approach here is to periodically back up
those databases as a whole, just like you would any other database.

If you want to be more fine-grained, you *may* decide to **not** backup the
database used by the HTTP JSON API Service instances. Note that it is
imperative that you still backup the databases for the other components (Trigger
Service and Daml driver for PostgreSQL) if you are running them.

If you are running the Helm chart solely for the HTTP JSON API Service
(connected to an external Ledger API server), then you can eschew backing up
entirely, as the database for the HTTP JSON API Service is an
easy-to-reconstruct cache. This assume that, in this setup, the data store of
the Ledger API server is, itself, properly backed up.

Secure Daml
-----------

The Helm chart assumes that the Kubernetes environment itself is trusted, and
as such does not encrypt connections between components. Full TLS encryption
between every component is not supported by the Helm chart. Individual
components do support it, so if that is a requirement for you you can still set
it up, though not through the Helm chart. Refer to the
`Secure Daml Infrastructure <https://github.com/digital-asset/ex-secure-daml-infra>`_
repository for guidance on how to set that up.

When using the Helm chart, we recommend against exposing the Ledger API gRPC
endpoint outside of the cluster, and exposing the HTTP JSON API Service,
Trigger Service, and OAuth2 Middleware endpoints only through an HTTP proxy.
That is why the services started by the Helm chart are of type ``ClusterIP``.

That proxy should either do TLS termination, or be itself behind a proxy that
does, in which case all of the communications between the TLS termination
endpoint must be happening on a fully trusted network.

See the :ref:`auth0` section for an example of setting up nginx to proxy
external connections to the JSON API, Trigger Service and OAuth2 Middleware.

Helm Chart Options Reference
----------------------------

..
  Copied from https://github.com/DACH-NY/connect-helm-chart/blob/c297baae3565d92f6ff2aad5e40b7138945772b5/Configuration.md
  TODO: this will need updating across repositories, which is time-consuming and error-prone,
        as some adjustment may be needed when converting from MD to RST -- we should evalute
        a different strategy to deal with this documentation.

These options have been extracted from the Helm chart version ``daml-connect-1.18.0-20211110.main.84.c297baae``.

authUrl
^^^^^^^

- **Type**: string
- **Required**: if either the ledger or the auth middleware is started

The JWKS endpoint used to get the public key to validate tokens. Used by the
ledger and the OAuth2 Middleware.

imagePullSecret
^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: true

The Kubernetes secret which is used for gaining access to the repository where
the Daml Docker images are located.

jsonApi.create
^^^^^^^^^^^^^^

- **Type**: bool
- **Default**: true
- **Required**: false

Controls whether the HTTP JSON API Service is deployed.

jsonApi.db.host
^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & production

The hostname of the database server for the HTTP JSON API Service, if one is
started by the Helm chart.

jsonApi.db.oracle.serviceName
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & using Oracle

If the HTTP JSON API Service database is Oracle, this is used to set the
Service Name.

jsonApi.db.port
^^^^^^^^^^^^^^^

- **Type**: integer
- **Required**: if enabled & production

The exposed port of the database server for the HTTP JSON API Service, if one
is started by the Helm chart.

jsonApi.db.postgres.database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & using an external PostgreSQL

The database the HTTP JSON API Service should use when connecting to the
database server.

jsonApi.db.secret
^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & production

The Kubernetes secret which is used for gaining access to the database.
The content should have the following structure:

.. code-block:: yaml

  username: daml
  password: s3cr3t

or as JSON:

.. code-block:: json

  {
      "username": "daml",
      "password": "s3cr3t"
  }

jsonApi.db.setupSecret
^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: none
- **Required**: false

The HTTP JSON API Service supports a mode where the credentials used at startup
(to create the database structure) are not the same as the credentials used
while the application is running. This can be useful if you want to run with
lower privileges, specifically without the privileges to alter table structure.

If this option is given, a separate instance of the HTTP JSON API Service will
be started with ``start-mode=create-only`` using these credentials as a
one-time job, while the regular, long-lived instances will be started with
``start-mode=start-only``.  If this option is **not** given, then no separate
one-time job is started and regular instances are started with
``start-mode=create-if-needed-and-start``.

The format of this option is the same as ``jsonApi.db.secret``.

jsonApi.healthCheck
^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: see below
- **Required**: false

Overrides the probes for the long-lived HTTP JSON API Service instances. The
current default is:

.. code-block:: yaml

    readinessProbe:
      httpGet:
        path: /readyz
        port: http
      initialDelaySeconds: 10
      periodSeconds: 5
    startupProbe:
      httpGet:
        path: /livez
        port: http
      failureThreshold: 30
      periodSeconds: 10
    livenessProbe:
      httpGet:
        path: /livez
        port: http
      initialDelaySeconds: 10
      failureThreshold: 1
      periodSeconds: 5

jsonApi.logLevel
^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: info
- **Required**: false

Sets the log level for the HTTP JSON API Service instances. Valid values are
``error``, ``warning``, ``info``, ``debug`` and ``trace``.

jsonApi.podAnnotations
^^^^^^^^^^^^^^^^^^^^^^

- **Type**: object
- **Default**: {}
- **Required**: false

The annotations which should be attached to the metadata of the HTTP JSON API
Service pods.

jsonApi.replicaCount
^^^^^^^^^^^^^^^^^^^^

- **Type**: number
- **Default**: 1
- **Required**: false

Controls how many long-lived instance of the HTTP JSON API Service are started.

jsonApi.resources
^^^^^^^^^^^^^^^^^

- **Type**: object
- **Default**: see below
- **Required**: false

Overrides the ``resources`` field on the HTTP JSON API Service pods. Default:

.. code-block:: yaml

    limits:
      cpu: "1"
      memory: "2Gi"
    requests:
      cpu: "0.5"
      memory: "1Gi"

jsonApi.serviceAccount
^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: null
- **Required**: false

The service account which should be attached to the HTTP JSON API Service pods.

ledger.create
^^^^^^^^^^^^^

- **Type**: bool
- **Default**: true
- **Required**: false

If true, the Helm chart will create a Daml driver for PostgreSQL instance. If
false, you will need to provide ``ledger.host`` and ``ledger.port`` (see
below).

ledger.db.host
^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & production

The hostname of the database server for the Daml driver for PostgreSQL, if one
is started by the Helm chart.

ledger.db.port
^^^^^^^^^^^^^^

- **Type**: integer
- **Required**: if enabled & production

The exposed port of the database server for the Daml driver for PostgreSQL, if
one is started by the Helm chart.

ledger.db.postgres.database
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & production

The database the Daml driver for PostgreSQL should use when connecting to the
database server. Note that, unlike the Trigger Service and HTTP JSON API
Service, the Daml driver for PostgreSQL started by the Helm chart only supports
PostgreSQL database servers.

ledger.db.secret
^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & production

The Kubernetes secret which is used for gaining access to the database.
The content should have the following structure:

.. code-block:: yaml

  username: daml
  password: s3cr3t

or as JSON:

.. code-block:: json

  {
      "username": "daml",
      "password": "s3cr3t"
  }

ledger.db.setupSecret
^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: none
- **Required**: false

The Daml driver for PostgreSQL supports two start modes: ``--migrate-only`` and
``--migrate-and-start``. The long-running instance always starts with
``--migrate-and-start``, but if you supply this option, the Helm chart will
start a separate, one-time job with ``--migrate-only``.

This can be used to supply separate credentials with table alteration
privileges to the one-time job (this property), and restricted credentials with
no table creation/alteration privileges to the long-running one
(``ledger.db.secret``).

The structure is the same as ``ledger.db.secret``.

ledger.healthCheck
^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: see below
- **Required**: false

Overrides the probes for the long-running Daml driver for PostgreSQL instance.
Defaults:

.. code-block:: yaml

    readinessProbe:
      exec:
        command: ["./grpc-health-probe", "-addr=:6865" ]
      initialDelaySeconds: 5
      failureThreshold: 30
      periodSeconds: 5
    livenessProbe:
      exec:
        command: ["./grpc-health-probe", "-addr=:6865" ]
      initialDelaySeconds: 10
      failureThreshold: 30
      periodSeconds: 5


ledger.host
^^^^^^^^^^^

- **Type**: string
- **Required**: if ledger.create is false

If the Helm chart should not create its own Daml driver for PostgreSQL instance
(i.e. you want to connect to other components to an existing gRPC Ledger API
provider), this option should be set to the hostname of the gRPC Ledger API
Server to connect to.

ledger.podAnnotations
^^^^^^^^^^^^^^^^^^^^^

- **Type**: object
- **Default**: {}
- **Required**: false

The annotations which should be attached to the metadata of the Daml driver for
PostgreSQL pod.

ledger.port
^^^^^^^^^^^

- **Type**: number
- **Default**: 6865
- **Required**: false

The port on which the external gRPC Ledger API Server is exposed.

ledger.resources
^^^^^^^^^^^^^^^^

- **Type**: object
- **Default**: see below
- **Required**: false

Overrides the ``resources`` field of the Daml driver for PostgreSQL pod.
Defaults:

.. code-block:: yaml

    limits:
      cpu: "1"
      memory: "2Gi"
    requests:
      cpu: "0.5"
      memory: "1Gi"

ledger.serviceAccount
^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: null
- **Required**: false

The service account which should be attached to the Daml driver for PostgreSQL
pod.

oauthMiddleware.callback
^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if oauthMiddleware.create

The ``--callback`` argument given to the :ref:`oauth2-middleware` instance.

oauthMiddleware.clientId
^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if oauthMiddleware.create

The value of the ``DAML_CLIENT_ID`` environment variable needed by the
:ref:`oauth2-middleware` instance.

oauthMiddleware.clientSecret
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if oauthMiddleware.create

The value of the ``DAML_CLIENT_SECRET`` environment variable needed by the
:ref:`oauth2-middleware` instance.

oauthMiddleware.create
^^^^^^^^^^^^^^^^^^^^^^

- **Type**: bool
- **Default**: false
- **Required**: false

Controls whether the OAuth2 Middleware should be deployed.

oauthMiddleware.healthCheck
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: see below
- **Required**: false

Overrides the probes for the OAuth2 Auth Middleware instance. Defaults:

.. code-block:: yaml

    startupProbe:
      httpGet:
        path: /livez
        port: http
      failureThreshold: 30
      periodSeconds: 10
    livenessProbe:
      httpGet:
        path: /livez
        port: http
      initialDelaySeconds: 10
      failureThreshold: 1
      periodSeconds: 5

oauthMiddleware.oauthAuth
^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: true

The ``oauth-auth`` argument given to the :ref:`oauth2-middleware` instance.

oauthMiddleware.oauthToken
^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: true

The ``oauth-token`` argument given to the :ref:`oauth2-middleware` instance.

oauthMiddleware.podAnnotations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: object
- **Default**: {}
- **Required**: false

The annotations which should be attached to the metadata of the OAuth2 Auth
Middleware pod.

oauthMiddleware.replicaCount
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: number
- **Default**: 1
- **Required**: false

Controls how many replicas of the OAuth2 Auth Middleware are started.

oauthMiddleware.resources
^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: object
- **Default**: see below
- **Required**: false

Overrides the ``resources`` field on the OAuth2 Auth Middleware pods. Defaults:

.. code-block:: yaml

    limits:
      cpu: "1"
      memory: "2Gi"
    requests:
      cpu: "0.5"
      memory: "1Gi"

oauthMiddleware.serviceAccount
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: not used
- **Required**: false

The service account which should be attached to the OAuth2 Auth Middleware pods.

production
^^^^^^^^^^

- **Type**: string
- **Default**: false
- **Required**: false

If true, disables the non-production components, and marks some important
options as required.

triggerService.authCallback
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: true

The ``--auth-callback`` argument passed to the :ref:`trigger-service` instance.
Note that this should be externally-reachable.

triggerService.authExternal
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: true

The ``--auth-external`` argument passed to the :ref:`trigger-service` instance.
Note that this should be externally-reachable.

triggerService.create
^^^^^^^^^^^^^^^^^^^^^

- **Type**: bool
- **Default**: false
- **Required**: false

Controls whether a Trigger Service instance should be created.

triggerService.db.host
^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & production

The hostname of the database server for the Trigger Service, if one is started
by the Helm chart.

triggerService.db.oracle.serviceName
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & using Oracle

If the Trigger Service database is Oracle, this is used to set the
Service Name.

triggerService.db.port
^^^^^^^^^^^^^^^^^^^^^^

- **Type**: integer
- **Required**: if enabled & production

The exposed port of the database server for the Trigger Service, if one is
started by the Helm chart.

triggerService.db.postgres.database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & using an external PostgreSQL

The database the Trigger Service should use when connecting to the
database server.

triggerService.db.secret
^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Required**: if enabled & production

The Kubernetes secret which is used for gaining access to the database.
The content should have the following structure:

.. code-block:: yaml

  username: daml
  password: s3cr3t

or as JSON:

.. code-block:: json

  {
      "username": "daml",
      "password": "s3cr3t"
  }


triggerService.db.setupSecret
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: null
- **Required**: false

The Trigger Service supports an optional argument ``init-db`` which, when
supplied, causes the Trigger Service to initialize its database structure and
rthen immediately exit. If this field is set, the Helm chart will start a
separate instance of the Trigger Service in this mode, as a one-time job.

This can be used to supply separate credentials with table alteration
privileges to the one-time job (this property), and restricted credentials with
no table creation/alteration privileges to the long-running one
(``triggerService.db.secret``).

The format of this option is the same as ``triggerService.db.secret``.

triggerService.healthCheck
^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: see below
- **Required**: false

Overrides the probes for the long-running Trigger Service instance. Defaults:

.. code-block:: yaml

    startupProbe:
      httpGet:
        path: /livez
        port: http
      failureThreshold: 30
      periodSeconds: 10
    livenessProbe:
      httpGet:
        path: /livez
        port: http
      initialDelaySeconds: 10
      failureThreshold: 1
      periodSeconds: 5

triggerService.podAnnotations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: object
- **Default**: {}
- **Required**: false

The annotations which should be attached to the metadata of the Trigger Service
pod.

triggerService.resources
^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: object
- **Default**: see below
- **Required**: false

Overrides the ``resources`` field of the Trigger Service pod. Defaults:

.. code-block:: yaml

    limits:
      cpu: "1"
      memory: "2Gi"
    requests:
      cpu: "0.5"
      memory: "1Gi"

triggerService.serviceAccount
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Type**: string
- **Default**: not used
- **Required**: false

The service account which should be attached to the Trigger Service pod.
