.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _connect-helm-chart:

Connect Helm Chart
==================

As of 1.18.0, we provide an Early Access version of the Connect Helm Chart for
Enterprise Edition customers. This page contains documentation for that Helm
chart.

Credentials
-----------

Like all Enterprise Edition components, the Helm Chart is hosted on
Artifactory. To get both the Helm chart itself and the Docker images it relies
on, you will need Artifactory credentials. In the rest of thie document, we
assume that ``$ARTIFACTORY_USERNAME`` refers to your Artifactory user name,
whereas ``$ARTIFACTORY_PASSWORD`` refers to your Artifactory API key.

Installing the Helm chart repository
------------------------------------

To let your local Helm installation know about the Daml Connect Helm chart, you
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

Setting up the ``imagePullSecret``
----------------------------------

The Helm chart relies on the production-ready Docker images for individual
components that are part of the Enterprise Edition. Specifically, it expects a
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
run this command after having added the Daml Connect Helm chart repository as
explained above.

This is going to start the following:

- For each of the state-keeping components (Daml Driver for PostgreSQL, HTTP
  JSON API Service), an "internal" PostgreSQL database server. These are
  decidedly not production-ready. For a production setup, you'll need to
  provide your own databases here.
- A fake, testing-only JWT minter to serve as the authentication server. This
  should be replaced with a real authentication server for production use. See
  the :ref:`auth0` section for an example of using an external authentication
  infrastructure.
- A single instance of each of the following services: Daml Driver for
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
  disabble the internal PostgreSQL instances, the mock auth server, and the
  reverse proxy.
- ``ledger.db``: If you want the Helm char to start a Daml Driver For
  PostgreSQL instance for you, you need to set this. See reference section at
  the end of this page for details.
- ``ledger.host`` and ``ledger.port``: If you **do not** want the Helm chart to
  setup a Daml Driver isntance for you, but instead want the components started
  by it to connect to an existing Ledger API server, fill in these options
  instead of the ``ledger.db`` object.
- ``jsonApi.db``: If you want the Helm chart to start the HTTP JSON API Service
  for you, you need to set this. See reference section at the end of this page
  for details.
- ``triggerService.db``: If you want the Helm chart to start the Trigger
  Service for you, you need to set this. See reference section at the end of
  this page for details.
- ``authUrl``: If you want the Helm chart to provide either a Daml Driver for
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

As of 1.18.0, the :ref:`HTTP JSON API <json-api>` component in the Helm chart produces
JSON-encoded logs. Other components log in somewhat less structured formats.

Daml Metrics Options
--------------------

The Daml Driver for PostgreSQL instance and the HTTP JSON API instances started
by the Helm chart are configured to expose Prometheus metrics on a port named
`metrics`, using the appropriate annotations. This means that, if you are
running a cluster-wide Prometheus instance, the relevant metrics should be
collected automatically.

See each component's documentation for details on the metrics exposed:

- `Daml Driver for PostgreSQL </daml-driver-for-postgresql/#types-of-metrics>`_
- :ref:`JSON API metrics <json-api-metrics>`

Upgrading
---------

.. note::

   This section only makes sense with the ``production`` flag set to ``true``.

Upgrading the Daml Connect version should be done by uninstalling the exiting
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

Backing Up
----------

.. note::

   This section only makes sense with the ``production`` flag set to ``true``.

For a production setup, you should be providing the Helm chart with external
database coordinates. The simplest approach here is to periodically back up
those databases as a whole, just like you would any other database.

If you want to be more fine-grained, you *may* decide to **not** backup the
database used by the HTTP JSON API Service instances. Note that it is
imperative that you still backup the databases for the other components (Trigger
Service and Daml Driver for PostgreSQL) if you are running them.

If you are running the Helm chart solely for the HTTP JSON API Service
(connected to an external Ledger API server), then you can eschew backing up
entirely, as the database for the HTTP JSON API Service is an
easy-to-reconstruct cache. This assume that, in this setup, the data store of
the Ledger API server is, itself, properly backed up.

Securing Daml Connect
---------------------

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

TODO: copy over Configuration.md
