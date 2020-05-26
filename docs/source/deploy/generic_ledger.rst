.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _deploy-generic-ledger:

Deploying to a generic DAML ledger
==================================

DAML ledgers expose a unified administration API. This means that deploying to a DAML ledger is no
different from deploying to your local sandbox.

To deploy to a DAML ledger, run the following command from within your DAML project:

.. code-block:: none

   $ daml deploy --host=<HOST> --port=<PORT> --access-token-file=<TOKEN-FILE>

where ``<HOST>`` and ``<PORT>`` is the hostname and port your ledger is listening on, which defaults
to port ``6564``. The ``<TOKEN-FILE>`` is needed if your sandbox runs with :ref:`authentication
<authentication>` and needs to contain a JWT token with an ``admin`` claim. If your sandbox is not
setup to use any authentication it can be omitted.

Instead of passing ``--host`` and ``--port`` flags to the command above, you can add the following
section to the project's ``daml.yaml`` file:

.. code-block:: yaml

   ledger:
       host: <HOSTNAME>
       port: <PORT>

The ``daml deploy`` command will

#. upload the project's compiled DAR file to the ledger. This will make the DAML templates defined
   in the current project available to the API users of the sandbox.

#. allocate the parties specified in the project's ``daml.yaml`` on the ledger if they are missing.

For more further interactions with the ledger, use the ``daml ledger`` command. Try running ``daml
ledger --help`` to get a list of available ledger commands:

.. code-block:: none

   $ daml ledger --help
   Usage: daml ledger COMMAND
     Interact with a remote DAML ledger. You can specify the ledger in daml.yaml
     with the ledger.host and ledger.port options, or you can pass the --host and
     --port flags to each command below. If the ledger is authenticated, you should
     pass the name of the file containing the token using the --access-token-file
     flag.

   Available options:
     -h,--help                Show this help text

   Available commands:
     list-parties             List parties known to ledger
     allocate-parties         Allocate parties on ledger
     upload-dar               Upload DAR file to ledger
     navigator                Launch Navigator on ledger

Connecting via TLS
------------------

To connect to the ledger via TLS, you can pass ``--tls`` to the
various commands. If your ledger supports or requires mutual
authentication you can pass your client key and certificate chain
files via ``--pem client_key.pem --crt client.crt``. Finally, you can
use a custom certificate authority for validating the server
certificate by passing ``--cacrt server.crt``. If ``--pem``, ``--crt``
or ``--cacrt`` are specified TLS is enabled automatically so ``--tls``
is redundant.
